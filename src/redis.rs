use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::iter::FromIterator;
use std::convert::{TryInto, TryFrom};
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use serde::{Serialize, Deserialize};

use redis::{IntoConnectionInfo, Commands};
pub use redis::{self, ToRedisArgs, FromRedisValue, RedisWrite};

use rand::Rng;

use derive_more::From;

use crate::cache::{Read, Validation, Validate, Write, ContentUpdate, Cached};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct NotFoundError(String);

#[derive(Debug)]
pub struct ParseError(String);

#[derive(Debug, From)]
pub enum Error {
    Redis(redis::RedisError),
    Internal(&'static str),
    Serde(serde_json::Error),
    Resource(Box<dyn std::error::Error>),
    NotFound(NotFoundError),
    Parse(ParseError)
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Redis(re) => {
                write!(f, "redis: {}", re)
            },
            Self::Internal(s) => {
                write!(f, "internal: {}", s)
            },
            Self::Serde(se) => {
                write!(f, "serde: {}", se)
            },
            Self::Resource(re) => {
                write!(f, "resource: {}", re)
            },
            Self::NotFound(NotFoundError(s)) => {
                write!(f, "not found: {}", s)
            },
            Self::Parse(ParseError(p)) => {
                write!(f, "parse: {}", p)
            }
        }
    }
}

impl std::error::Error for Error {}

lazy_static! {
    static ref BORROW_MUT_SCRIPT: redis::Script = {
        let src = "\
        local generation = redis.call('HGET', KEYS[1], 'generation') \
        if generation then \
          if ARGV[1] ~= generation then \
            return redis.err_reply('invalid generation') \
          else \
            if redis.call('HGET', KEYS[1], 'lock') then \
              return redis.err_reply('lock already locked') \
            else \
              local lock_id = redis.call('HINCRBY', KEYS[1], 'lock_seed', 1) \
              redis.call('HSET', KEYS[1], 'lock', lock_id) \
              local data = redis.call('HGET', KEYS[1], 'data') \
              if data then \
                return {KEYS[1], generation, tostring(lock_id), data} \
              else \
                return {KEYS[1], generation, tostring(lock_id)} \
              end \
            end \
          end \
        else \
          return redis.err_reply('invalid entry not declared') \
        end \
        ";
        redis::Script::new(src)
    };
    static ref GET_SCRIPT: redis::Script = {
        let src = "\
        local generation = redis.call('HGET', KEYS[1], 'generation') \
        if generation then \
          if ARGV[1] ~= generation then \
            return redis.err_reply('invalid generation') \
          else \
            local version = redis.call('HGET', KEYS[1], 'version') \
            local data = redis.call('HGET', KEYS[1], 'data') \
            if ARGV[2] >= version then \
              return {generation, version, 'unchanged'} \
            else \
              if data then \
                return {generation, version, 'value', data} \
              else \
                return {generation, version, 'removed'} \
              end \
            end \
          end \
        else \
          return redis.error_reply('invalid entry not declared')
        end";
        redis::Script::new(src)
    };
    static ref SET_RELEASE_SCRIPT: redis::Script = {
        let src = "\
        local generation = redis.call('HGET', KEYS[1], 'generation') \
        if generation then \
          if ARGV[1] ~= generation then \
            return redis.err_reply('invalid generation') \
          else \
            local lock = redis.call('HGET', KEYS[1], 'lock') \
            if lock then \
              if ARGV[2] == lock then \
                if ARGV[3] then \
                  redis.call('HSET', KEYS[1], 'data', ARGV[3]) \
                else \
                  redis.call('HDEL', KEYS[1], 'data') \
                end \
                redis.call('HINCRBY', KEYS[1], 'version', 1) \
                redis.call('HDEL', KEYS[1], 'lock') \
                return \
              else \
                return redis.err_reply('invalid lock') \
              end \
            else \
              return redis.err_reply('invalid entry not locked') \
            end \
          end \
        else \
          return redis.err_reply('invalid entry not declared') \
        end \
        ";
        redis::Script::new(src)
    };
    static ref RELEASE_SCRIPT: redis::Script = {
        let src = "\
        local generation = redis.call('HGET', KEYS[1], 'generation') \
        if generation then \
          if ARGV[1] ~= generation then \
            return redis.err_reply('invalid generation') \
          else \
            local lock = redis.call('HGET', KEYS[1], 'lock') \
            if lock then \
              if ARGV[2] == lock then \
                return redis.call('HDEL', KEYS[1], 'lock') \
              else \
                return redis.err_reply('invalid lock') \
              end \
            else \
              return redis.err_reply('invalid entry not locked') \
            end \
          end \
        else \
          return redis.err_reply('invalid entry not declared') \
        end \
        ";
        redis::Script::new(src)
    };
    static ref ALLOC_SCRIPT: redis::Script = {
        let src = "\
        local generation = redis.call('HGET', KEYS[1], 'generation') \
        if not generation then \
           redis.call('HMSET', KEYS[1], 'generation', ARGV[1], 'version', 1) \
        end \
        return redis.call('HMGET', KEYS[1], 'generation', 'version') \
        ";
        redis::Script::new(src)
    };
}

/// Connection parameters for redis.
pub struct StoreBuilder {
    /// redis instance url (defaults to redis://localhost:6379)
    url: String,
    /// redis password
    password: Option<String>
}

impl StoreBuilder {
    pub fn url(mut self, url: &str) -> Self {
        self.url = url.to_owned();
        self
    }

    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.to_owned());
        self
    }

    /// Get a `Store` with redis as the underlying
    pub fn build(self) -> Result<Store> {
        let mut conn_info = self.url.into_connection_info()?;
        conn_info.passwd = self.password;
        Ok(Store(Arc::new(redis::Client::open(conn_info)?)))
    }
}

impl Default for StoreBuilder {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".to_string(),
            password: None
        }
    }
}

/// A store represents an underlying persistent store used to back **jac**
#[derive(Clone)]
pub struct Store(Arc<redis::Client>);

impl Default for Store {
    fn default() -> Self {
        StoreBuilder::default().build().unwrap()
    }
}

impl Store {
    pub fn builder() -> StoreBuilder {
        StoreBuilder::default()
    }

    /// Supply a redis connection to the `Store`
    fn with_conn<F, U>(&self, f: F) -> Result<U>
    where
        F: FnOnce(redis::Connection) -> Result<U>,
    {
        let conn = self.0.get_connection()?;
        f(conn)
    }

    pub fn list<R, RV>(&self, pattern: R) -> Result<impl Iterator<Item = RV>>
    where
        R: ToRedisArgs,
        RV: FromRedisValue
    {
        self.with_conn(|mut conn| {
            let buf: Vec<_> = conn.scan_match(pattern)?
                .collect();
            Ok(buf.into_iter())
        })
    }

    /// attempts to pull `key` in store. If `key` does not exist, errors out.
    /// If it does, generation is compared with existing. If gen > existing gen
    /// then returns `CachedValue::New` otherwise `CachedValue::NoChange`
    pub fn validate<K, T>(
        &self,
        key: K,
        generation: &str,
        version: u64
    ) -> Result<Validation<u64, T>>
    where
        for<'a> T: Deserialize<'a>,
        K: ToRedisArgs
    {
        self.with_conn(|mut conn| {
            let mut resp: Vec<String> = GET_SCRIPT
                .prepare_invoke()
                .key(key)
                .arg(generation)
                .arg(version)
                .invoke(&mut conn)?;

            resp.reverse();

            resp.pop();  // generation

            let version: u64 = resp.pop()
                .ok_or(Error::Internal("script did not yield a new version"))?
                .parse()
                .map_err(|_| Error::Internal("script returned an invalid generation"))?;

            let res_type = resp.pop()
                .ok_or(Error::Internal("script did not yield resp type"))?;

            let update = match res_type.as_str() {
                "unchanged" => Ok(ContentUpdate::Unchanged),
                "removed" => Ok(ContentUpdate::Removed),
                "value" => {
                    let value_str = resp.pop()
                        .ok_or(Error::Internal("script did not yield data"))?;
                    let value = serde_json::from_str(&value_str)?;
                    Ok(ContentUpdate::Value(value))
                }
                _ => Err(Error::Internal("script resp type is invalid"))
            }?;

            Ok(Validation { version, update })
        })
    }

    /// Acquire a mut borrow on data. Errors out if key not controlled.
    pub fn get_mut<T, K>(
        &self,
        at: K,
        generation: &str
    ) -> Result<Lock<T>>
    where
        for<'b> T: Deserialize<'b>,
        K: ToRedisArgs
    {
        self.with_conn(|mut conn| {
            let mut resp: Vec<String> = BORROW_MUT_SCRIPT
                .prepare_invoke()
                .key(at)
                .arg(generation)
                .invoke(&mut conn)?;
            resp.reverse();
            Lock::<_>::try_new(resp)
        })
    }

    /// Get a key-value `StoreEntry` give a key `K`
    pub fn entry<K, T>(
        &self,
        at: K
    ) -> Result<StoreEntry<K, T>>
    where
        K: ToRedisArgs + Clone
    {
        self.with_conn(|mut conn| {
            let seed = base64::encode(&rand::thread_rng().gen::<[u8; 32]>());
            let (generation, _): (String, u64) = ALLOC_SCRIPT
                .prepare_invoke()
                .key(at.clone())
                .arg(seed)
                .invoke(&mut conn)?;
            Ok(StoreEntry {
                store: self.clone(),
                key: at,
                generation,
                _phantom: PhantomData
            })
        })
    }

    /// Release a lock without triggering a version bump
    fn release<K>(
        &self,
        key: K,
        generation: &str,
        id: i64
    ) -> Result<()>
    where
        K: ToRedisArgs
    {
        self.with_conn(|mut conn| {
            RELEASE_SCRIPT
                .prepare_invoke()
                .key(key)
                .arg(generation)
                .arg(id)
                .invoke(&mut conn)?;
            Ok(())
        })
    }

    /// Uses a borrow guard to set the data at the corresponding location
    pub fn set_release<K, T>(
        &self,
        key: K,
        generation: &str,
        id: i64,
        data: &Option<T>
    ) -> Result<()>
    where
        for<'a> T: Serialize + Deserialize<'a>,
        K: ToRedisArgs
    {
        self.with_conn(|mut conn| {
            let mut invok = SET_RELEASE_SCRIPT.prepare_invoke();

            invok
                .key(key)
                .arg(generation)
                .arg(id);

            match data.as_ref() {
                Some(data) => {
                    let obj = serde_json::to_string(data)?;
                    invok.arg(obj);
                },
                None => {}
            };

            invok.invoke(&mut conn)?;

            Ok(())
        })
    }
}

/// A global `Lock` to write some data of type `T` in a given store
///
/// Only one instance of a write lock on an entry may exist at any one time.
/// However, contrary to a Mutex lock, several readers may exist concurrently
/// to a writer.
pub struct Lock<T> {
    id: i64,
    key: String,
    generation: String,
    data: Box<Option<T>>
}

impl<T> std::ops::Deref for Lock<T> {
    type Target = Option<T>;
    fn deref(&self) -> &Option<T> {
        self.data.deref()
    }
}

impl<T> std::ops::DerefMut for Lock<T> {
    fn deref_mut(&mut self) -> &mut Option<T> {
        self.data.deref_mut()
    }
}

impl<T> Lock<T>
where
    for<'b> T: Deserialize<'b>
{
    fn try_new(mut resp: Vec<String>) -> Result<Self> {
        let key = resp.pop()
            .ok_or(Error::Internal("borrow script did not yield the key"))?;

        let generation = resp.pop()
            .ok_or(Error::Internal("borrow script did not yield the generation"))?;

        let id = resp.pop()
            .ok_or(Error::Internal("borrow script did not yield a lock_id"))?
            .parse()
            .map_err(|_| Error::Internal("lock_id is not i64"))?;

        let data = resp.pop()
            .map(|data_str| {
                serde_json::from_str(&data_str).map_err(|e| Error::Serde(e))
            })
            .transpose()?;

        Ok(Self {
            generation,
            id,
            key,
            data: Box::new(data)
        })
    }
}

/// Similar to the entry pattern (like  std::collections::hash_map::Entry)
/// Holds a key `K` to a value `V in the context of a `Store`
pub struct StoreEntry<K, T> {
    store: Store,
    key: K,
    generation: String,
    _phantom: PhantomData<T>
}

impl<K, T> Validate for StoreEntry<K, T>
where
    K: ToRedisArgs + Clone,
    for<'b> T: Serialize + Deserialize<'b>
{
    type Item = T;

    type Error = Error;

    type Version = u64;

    fn validate(&self, version: u64) -> Result<Validation<u64, Self::Item>> {
        self.store.validate(self.key.clone(), &self.generation, version)
    }

    fn refresh(&self) -> Result<(u64, Option<Self::Item>)> {
        let Validation { version, update } = self.store.validate(
            self.key.clone(),
            &self.generation,
            0
        )?;
        match update {
            ContentUpdate::Value(v) => Ok((version, Some(v))),
            ContentUpdate::Removed => Ok((version, None)),
            ContentUpdate::Unchanged => Err(Error::Internal("invalid response"))
        }
    }
}

use crate::cache::WriteError;

impl<K, T> Write<T> for StoreEntry<K, T>
where
    K: ToRedisArgs + Clone,
    for<'a> T: Deserialize<'a> + Serialize
{
    type Error = Error;

    type Lock = Lock<T>;

    fn write(&self) -> Result<Self::Lock> {
        self.store.get_mut(self.key.clone(), &self.generation)
    }

    fn push(&self, lock: Self::Lock) ->
        std::result::Result<(), WriteError<Self::Lock, Self::Error>>
    {
        self.store
            .set_release(&lock.key, &lock.generation, lock.id, &lock.data)
            .map_err(|error| WriteError { lock, error })
    }

    fn abort(&self, lock: Self::Lock) ->
        std::result::Result<(), WriteError<Self::Lock, Self::Error>>
    {
        self.store
            .release(&lock.key, &lock.generation, lock.id)
            .map_err(|error| WriteError { lock, error })
    }
}

pub type CachedEntry<K, V> = Cached<StoreEntry<K, V>>;

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::cache::{Cached, Write, OwnedIntent};

    use rand::prelude::*;

    fn mk_store() -> Store {
        StoreBuilder::new("redis://localhost:6379", None)
            .build()
    }

    fn mk_key() -> String {
        let mut buf = Vec::<u8>::new();
        for _ in (1..64) {
            let r: [u8; 32] = rand::thread_rng().gen();
            buf.extend(&r);
        }
        base64::encode(&buf)
    }

    #[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
    struct TestModel {
        #[serde(flatten)]
        a_hash_map: std::collections::HashMap<String, String>
    }

    #[test]
    fn cache() {
        let store = mk_store();
        let key = mk_key();

        let entry = store.entry::<_, TestModel>(&key)
            .unwrap()
            .map(|model| model.a_hash_map.len())
            .into_cached()
            .unwrap();

        let my_model = TestModel { a_hash_map: HashMap::new() };

        entry
            .write_with(|mut guard| {
                *guard = Some(my_model.clone());
                guard.apply()
            })
            .unwrap();

        entry.read_with(|v| assert_eq!(*v, 0))
            .unwrap();
    }

    #[test]
    fn locking_twice_not_allowed() {
        let store = mk_store();
        let key = mk_key();
        let entry = store.entry::<_, TestModel>(&key)
            .unwrap()
            .into_cached()
            .unwrap();
        entry.write_with(|guard| {
            match entry.write_with(|guard_p| guard_p.discard()) {
                Ok(_) => panic!("locking twice not allowed"),
                _ => ()
            };
            guard.discard()
        }).unwrap();
    }
}
