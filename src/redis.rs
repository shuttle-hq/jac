use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::iter::FromIterator;
use std::convert::{TryInto, TryFrom};
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use serde::{Serialize, Deserialize};

use redis::{IntoConnectionInfo, Commands};
pub use redis::{ToRedisArgs, FromRedisValue, RedisWrite};

use rand::Rng;

use derive_more::From;

use crate::cache::{Cache, CacheError, Validation, Validate, Write, OwnedIntent, ContentUpdate, Cached};

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

impl Error {
    pub fn into_cache_err(self) -> CacheError<Self> {
        CacheError::Backend(self)
    }
}

impl From<Error> for CacheError<Error> {
    fn from(err: Error) -> Self {
        CacheError::Backend(err)
    }
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

pub struct RedisOpt {
    url: String,
    password: Option<String>
}

impl RedisOpt {
    pub fn new(url: &str, password: Option<&str>) -> Self {
        Self {
            url: url.to_string(),
            password: password.map(|p| p.to_string())
        }
    }
    pub fn to_client(&self) -> Result<redis::Client> {
        let mut conn_info = self.url.clone().into_connection_info()?;
        conn_info.passwd = self.password.clone();
        Ok(redis::Client::open(conn_info)?)
    }
    pub fn build(self) -> Store {
        Store::new(&self)
    }
}

impl Default for RedisOpt {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".to_string(),
            password: None
        }
    }
}

#[derive(Clone)]
pub struct Store(Arc<redis::Client>);

impl Default for Store {
    fn default() -> Self {
        Store::new(&RedisOpt::default())
    }
}

impl Store {
    pub fn new(opt: &RedisOpt) -> Self {
        Self(Arc::new(opt.to_client().expect("invalid redis config")))
    }

    fn with_conn<F, U>(&self, f: F) -> Result<U>
    where
        F: FnOnce(redis::Connection) -> Result<U>,
    {
        let conn = self.0.get_connection()?;
        f(conn)
    }

    pub fn list<V: FromStr, I: FromIterator<V>>(&self, pattern: &str) -> Result<I>
    {
        self.with_conn(|mut conn| {
            conn.scan_match(pattern)?
                .map(|k: String| k.parse().map_err(|_| ParseError(k).into()))
                .collect()
        })
    }

    pub fn mount<I, K, V>(&self, at: I) -> CachedStore<K, V>
    where
        K: ToRedisArgs + Eq + Hash + Clone,
        for<'a> V: Deserialize<'a> + Serialize,
        I: AsRef<str>
    {
        CachedStore::<K, V>::new_with_prefix(self.clone(), at.as_ref())
    }

    /// attempts to pull `key` in store. If `key` does not exist, errors out.
    /// If it does, generation is compared with existing. If gen > existing gen
    /// then returns CachedValue::New otherwise CachedValue::NoChange
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
    ) -> Result<LockGuard<T>>
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
            LockGuard::<_>::try_new(self, resp)
        })
    }

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
                generation: generation,
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum LockGuardOp {
    NoChange,
    Change
}

pub struct LockGuard<T> {
    store: Store,
    id: i64,
    key: String,
    generation: String,
    op: LockGuardOp,
    pub data: Box<Option<T>>
}

impl<T> std::ops::Deref for LockGuard<T> {
    type Target = Option<T>;
    fn deref(&self) -> &Option<T> {
        self.data.deref()
    }
}

impl<T> std::ops::DerefMut for LockGuard<T> {
    fn deref_mut(&mut self) -> &mut Option<T> {
        self.data.deref_mut()
    }
}

impl<T> OwnedIntent<T> for LockGuard<T>
where
    T: Serialize
{
    type Op = Self;

    fn apply(mut self) -> Self::Op {
        self.op = LockGuardOp::Change;
        self
    }

    fn discard(mut self) -> Self::Op {
        self.op = LockGuardOp::NoChange;
        self
    }
}

impl<T> LockGuard<T>
where
    for<'b> T: Deserialize<'b>
{
    fn try_new(store: &Store, mut resp: Vec<String>) -> Result<Self> {
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
            store: store.clone(),
            generation,
            id,
            key,
            op: LockGuardOp::NoChange,
            data: Box::new(data)
        })
    }
}

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

impl<K, T> Write<T> for StoreEntry<K, T>
where
    K: ToRedisArgs + Clone,
    for<'a> T: Deserialize<'a> + Serialize
{
    type Error = Error;

    type Intent = LockGuard<T>;

    fn try_write(&self) -> Result<Self::Intent> {
        self.store.get_mut(self.key.clone(), &self.generation)
    }

    fn apply(
        &self,
        guard: LockGuard<T>
    ) -> Result<()> {
        match guard.op {
            LockGuardOp::NoChange =>
                self.store.release(&guard.key, &guard.generation, guard.id),
            LockGuardOp::Change =>
                self.store.set_release(&guard.key, &guard.generation, guard.id, &guard.data)
        }
    }
}

pub struct RHashMap<K, V> {
    prefix: Option<String>,
    store: Store,
    inner: Arc<Mutex<HashMap<K, V>>>
}

pub struct Prefixed<'a, K> {
    prefix: &'a Option<String>,
    key: &'a K
}

impl<'a, K> ToRedisArgs for Prefixed<'a, K>
where
    K: ToRedisArgs
{
    fn write_redis_args<W: ?Sized>(&self, out: &mut W)
    where
        W: redis::RedisWrite
    {
        match self.prefix {
            Some(prefix) => prefix.write_redis_args(out),
            _ => ()
        };
        self.key.write_redis_args(out);
    }
}

pub type CachedEntry<K, V> = Cached<StoreEntry<K, V>>;

impl<K, V> RHashMap<K, CachedEntry<K, V>>
where
    K: ToRedisArgs + Eq + Hash + Clone,
    for<'a> V: Deserialize<'a> + Serialize
{
    pub fn new(store: Store) -> Self {
        Self {
            prefix: None,
            store: store,
            inner: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub fn new_with_prefix(store: Store, prefix: &str) -> Self {
        let mut out = Self::new(store);
        out.prefix = Some(prefix.to_owned());
        out
    }

    fn with_prefix<'a>(&'a self, k: &'a K) -> Prefixed<'a, K> {
        Prefixed {
            prefix: &self.prefix,
            key: k
        }
    }

    pub fn entry(&self, k: &K) -> std::result::Result<CachedEntry<K, V>, CacheError<Error>> {
        let mut inner = self.inner.lock()?;
        match inner.get(k) {
            Some(ce) => Ok((*ce).clone()),
            None => {
                let entry: CachedEntry<K, V> = self.store.entry(k.clone())?.into_cached()?;
                inner.insert(k.clone(), entry.clone());
                Ok(entry)
            }   
        }
    }
}

pub type CachedStore<K, V> = RHashMap<K, CachedEntry<K, V>>;

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::cache::{Cached, Write, OwnedIntent};

    use rand::prelude::*;

    fn mk_store() -> Store {
        RedisOpt::new("redis://localhost:6379", None)
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

        let entry: Cached<_> = store.entry::<_, TestModel>(&key)
            .unwrap()
            .map(|model| model.a_hash_map.len())
            .into_cached()
            .unwrap();

        let my_model = TestModel { a_hash_map: HashMap::new() };

        entry
            .try_write_with(|mut guard| {
                *guard = Some(my_model.clone());
                guard.apply()
            })
            .unwrap();

        entry.try_read_with(|v| assert_eq!(*v, 0))
            .unwrap();
    }

    #[test]
    fn locking_twice_not_allowed() {
        let store = mk_store();
        let key = mk_key();
        let entry: Cached<_> = store.entry::<_, TestModel>(&key)
            .unwrap()
            .into_cached()
            .unwrap();
        entry.try_write_with(|guard| {
            match entry.try_write_with(|guard_p| guard_p.discard()) {
                Ok(_) => panic!("locking twice not allowed"),
                _ => ()
            };
            guard.discard()
        }).unwrap();
    }
}
