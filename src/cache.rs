//! TODO: Some macros for impls on Arc<..>

use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{Mutex, RwLock, Arc};
use std::time::{Duration, Instant};
use std::ops::DerefMut;
use std::sync::RwLockReadGuard;
use std::convert::Infallible;

/// A wrapper around content returned as part of an validation
/// strategy.
#[derive(Clone)]
pub enum ContentUpdate<T> {
    /// The content has not changed
    Unchanged,
    /// The content has been removed
    Removed,
    /// The content has changed and the new value is `T`
    Value(T)
}

/// A validation token
pub struct Validation<V, T> {
    /// The current version of the content
    pub version: V,
    /// The wrapped content
    pub update: ContentUpdate<T>
}

/// A validation strategy. `V` is the type used as version.
pub trait Validate: Sized {
    /// The type of the controlled content
    type Item;

    /// The type of error for the validation strategy
    type Error: std::error::Error;

    /// The type to be used as the version of the controlled content
    type Version: Clone;

    /// `validate` content at `version`. This should be quick to do and only rarely return
    /// new content.
    fn validate(
        &self,
        version: Self::Version
    ) -> Result<Validation<Self::Version, Self::Item>, Self::Error>;

    /// Get the latest `version` of content with it's current `version`
    fn refresh(&self) -> Result<(Self::Version, Option<Self::Item>), Self::Error>;

    /// applies a closure `f` to content of `self` before yielding new values
    /// on update, but otherwise respecting the inner validation strategy
    fn map<F, O>(self, f: F) -> ValidateMap<Self, F, O>
    where
        F: Fn(Self::Item) -> O
    {
        ValidateMap {
            inner: self,
            closure: f,
            _output: PhantomData
        }
    }

    /// returns the content wrapped in a `TimeToLive` object with the specified `ttl`
    fn time_to_live(self, ttl: Duration) -> TimeToLive<Self> {
        TimeToLive {
            inner: self,
            ttl,
            last: Mutex::new(None)
        }
    }

    /// transform the validation strategy into a cached object
    fn into_cached(self) -> Result<Cached<Self>, Self::Error> {
        let (version, data) = self.refresh()?;
        Ok(
            Cached {
                strategy: self,
                content: Arc::new(RwLock::new(Content { version, data }))
            }
        )
    }
}

#[derive(Debug)]
pub struct Versioned<V, T> {
    item: Option<T>,
    version: V
}

impl<V, T> Clone for Versioned<V, T>
where
    V: Clone,
    T: Clone
{
    fn clone(&self) -> Self {
        Self {
            item: self.item.clone(),
            version: self.version.clone(),
        }
    }
}

impl<V, T> Validate for Versioned<V, T>
where
    T: Clone,
    V: Clone + Eq
{
    type Item = T;
    type Error = Infallible;
    type Version = V;
    fn validate(
        &self,
        version: Self::Version
    ) -> Result<Validation<Self::Version, Self::Item>, Self::Error> {
        let res = if self.version != version {
            Validation {
                version: self.version.clone(),
                update: match self.item.as_ref() {
                    Some(item) => ContentUpdate::Value(item.clone()),
                    None => ContentUpdate::Removed
                }
            }
        } else {
            Validation {
                version,
                update: ContentUpdate::Unchanged
            }
        };
        Ok(res)
    }
    fn refresh(&self) -> Result<(Self::Version, Option<Self::Item>), Self::Error> {
        Ok((self.version.clone(), self.item.clone()))
    }
}

impl<V, T> Read for Versioned<V, T>
where
    T: Clone,
    V: Clone + Eq
{
    fn read_with<F, O>(&self, f: F) -> Result<Option<O>, Self::Error>
    where
        F: FnOnce(&Self::Item) -> O
    {
        let res = self.item.as_ref().map(f);
        Ok(res)
    }
}

pub type Constant<T> = Versioned<(), T>;

impl<T> Versioned<(), T>
where
    T: Clone,
{
    pub fn from_value(value: T) -> Self {
        Self { item: Some(value), version: () }
    }
}

impl<V, T> Versioned<V, T>
where
    T: Clone,
    V: Clone + Eq
{
    pub fn from_value_at_version(value: T, version: V) -> Self {
        Self { item: Some(value), version }
    }

    pub fn none_at_version(version: V) -> Self {
        Self { item: None, version }
    }
}

impl<V, T> Versioned<V, T> {
    pub fn read(&self) -> Option<&T> {
        self.item.as_ref()
    }
}

/// a TTL wrapper around a validation strategy `S`
pub struct TimeToLive<S: Validate> {
    inner: S,
    ttl: Duration,
    last: Mutex<Option<S::Version>>
}

/// a TTL validation strategy
impl<S> Validate for TimeToLive<S>
where
    S: Validate
{
    /// the type of the controlled content
    type Item = S::Item;

    /// the error for the validation strategy
    type Error = S::Error;

    /// the version of the validation strategy. Since it is a TTL validation strategy, the version is an instant.
    type Version = Instant;

    /// `validate` the current `version` by comparing the time elapsed against the `ttl`.
    fn validate(
        &self,
        version: Self::Version
    ) -> Result<Validation<Self::Version, Self::Item>, Self::Error> {
        if version.elapsed() > self.ttl {
            let mut last = self.last.lock().unwrap();
            let version = std::mem::replace(&mut *last, None);

            match version {
                Some(version) => {
                    let Validation {
                        version,
                        update
                    } = self.inner.validate(version)?;

                    *last = Some(version);

                    Ok(Validation {
                        version: Instant::now(),
                        update
                    })
                },
                None => {
                    let (version, item) = self.inner.refresh()?;

                    *last = Some(version);

                    Ok(Validation {
                        version: Instant::now(),
                        update: match item {
                            Some(item) => ContentUpdate::Value(item),
                            None => ContentUpdate::Removed
                        }
                    })
                }
            }
        } else {
            Ok(Validation {
                version,
                update: ContentUpdate::Unchanged
            })
        }
    }

    /// Get the latest version of content as dictated by the `inner` validation strategy
    /// (even if the TTL has not elapsed).
    fn refresh(&self) -> Result<(Self::Version, Option<Self::Item>), Self::Error> {
        let mut last = self.last.lock().unwrap();
        let (version, item) = self.inner.refresh()?;

        *last = Some(version);

        Ok((Instant::now(), item))
    }
}

pub struct ValidateMap<T, F, O> {
    inner: T,
    closure: F,
    _output: PhantomData<O>
}

impl<T, F, O> Validate for ValidateMap<T, F, O>
where
    T: Validate,
    F: Fn(T::Item) -> O
{
    type Item = O;
    type Error = T::Error;
    type Version = T::Version;

    /// `validate` content at `version` using the `inner` invalidation strategy
    fn validate(
        &self,
        version: Self::Version
    ) -> Result<Validation<Self::Version, Self::Item>, Self::Error> {
        let Validation { version, update } = self.inner.validate(version)?;
        Ok(
            Validation {
                version,
                update: match update {
                    ContentUpdate::Value(v) => ContentUpdate::Value((self.closure)(v)),
                    ContentUpdate::Unchanged => ContentUpdate::Unchanged,
                    ContentUpdate::Removed => ContentUpdate::Removed
                }
            }
        )
    }
    /// gets the latest version of the content as dictated by the `inner` validation strategy
    fn refresh(&self) -> Result<(Self::Version, Option<Self::Item>), Self::Error> {
        let (version, value) = self.inner.refresh()?;
        Ok((version, value.map(|v| (self.closure)(v))))
    }
}

pub struct Content<S>
where
    S: Validate
{
    version: S::Version,
    data: Option<S::Item>
}

/// A wrapper around a validation strategy that caches its content
pub struct Cached<S: Validate> {
    strategy: S,
    content: Arc<RwLock<Content<S>>>,
}

pub trait Read: Validate {
    fn read_with<F, O>(&self, f: F) -> Result<Option<O>, Self::Error>
    where
        F: FnOnce(&Self::Item) -> O;
}

impl<S> Cached<S>
where
    S: Validate,
    S::Version: Clone,
    S::Item: Clone
{
    pub fn clone_inner(&self) -> Result<Option<S::Item>, S::Error> {
        self.read_with(|inner| inner.clone())
    }
}

impl<S> Cached<S>
where
    S: Validate,
    S::Version: Clone
{
    fn validate(&self) -> Result<RwLockReadGuard<'_, Content<S>>, S::Error> {
        let res = {
            let content = self.content.read().unwrap();
            let res = self.strategy.validate(content.version.clone())?;
            match &res.update {
                ContentUpdate::Unchanged => return Ok(content),
                _ => Ok(res)
            }
        }?; 

        match res.update {
            ContentUpdate::Unchanged => {},
            ContentUpdate::Removed => {
                let mut content = self.content.write().unwrap();
                content.version = res.version;
                content.data = None;
            },
            ContentUpdate::Value(v) => {
                let mut content = self.content.write().unwrap();
                content.version = res.version;
                content.data = Some(v);
            },
        };

        let content = self.content.read().unwrap();
        Ok(content)
    }
}

impl<S> Read for Cached<S>
where
    S: Validate,
{
    fn read_with<F, O>(&self, f: F) -> Result<Option<O>, S::Error>
    where
        F: FnOnce(&S::Item) -> O
    {
        Ok(self.validate()?.data.as_ref().map(|inner| f(inner)))
    }
}

pub struct WriteError<L, E> {
    pub error: E,
    pub lock: L
}

impl<L, E> std::fmt::Debug for WriteError<L, E>
where
    E: std::error::Error
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <E as std::fmt::Debug>::fmt(&self.error, f)
    }
}

impl<L, E> std::fmt::Display for WriteError<L, E>
where
    E: std::error::Error
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <E as std::fmt::Display>::fmt(&self.error, f)
    }
}

impl<L, E> std::error::Error for WriteError<L, E>
where
    E: std::error::Error
{}

pub trait Write<T>: Sized {
    type Error: std::error::Error;
    type Lock: DerefMut<Target = Option<T>>;

    fn write(&self) -> Result<Self::Lock, Self::Error>;
    fn push(&self, lock: Self::Lock) -> Result<(), WriteError<Self::Lock, Self::Error>>;
    fn abort(&self, lock: Self::Lock) -> Result<(), WriteError<Self::Lock, Self::Error>>;
}

impl<T, I, F, O> Write<T> for ValidateMap<I, F, O>
where
    I: Write<T>
{
    type Error = I::Error;

    type Lock = I::Lock;

    fn write(&self) -> Result<Self::Lock, Self::Error> {
        self.inner.write()
    }

    fn push(&self, lock: Self::Lock) -> Result<(), WriteError<Self::Lock, Self::Error>> {
        self.inner.push(lock)
    }

    fn abort(&self, lock: Self::Lock) -> Result<(), WriteError<Self::Lock, Self::Error>> {
        self.inner.abort(lock)
    }
}

impl<I, S> Write<I> for Cached<S>
where
    S: Write<I> + Validate
{
    type Error = <S as Write<I>>::Error;

    type Lock = <S as Write<I>>::Lock;

    fn write(&self) -> Result<Self::Lock, Self::Error> {
        self.strategy.write()
    }

    fn push(&self, lock: Self::Lock) -> Result<(), WriteError<Self::Lock, Self::Error>> {
        self.strategy.push(lock)
    }

    fn abort(&self, lock: Self::Lock) -> Result<(), WriteError<Self::Lock, Self::Error>> {
        self.strategy.abort(lock)
    }
}

impl<S> Validate for Cached<S>
where
    S: Validate
{
    type Item = S::Item;
    type Error = S::Error;
    type Version = S::Version;

    fn validate(
        &self,
        version: Self::Version
    ) -> Result<Validation<Self::Version, Self::Item>, Self::Error> {
        self.strategy.validate(version)
    }

    fn refresh(&self) -> Result<(Self::Version, Option<Self::Item>), Self::Error> {
        self.strategy.refresh()
    }
}

impl<S> Read for Arc<S>
where
    S: Read
{
    fn read_with<F, O>(&self, f: F) -> Result<Option<O>, S::Error>
    where
        F: FnOnce(&S::Item) -> O
    {
        <S as Read>::read_with(self, f)
    }
}

impl<S, T> Write<T> for Arc<S>
where
    S: Write<T>
{
    type Error = S::Error;

    type Lock = S::Lock;

    fn write(&self) -> Result<Self::Lock, Self::Error> {
        <S as Write<T>>::write(self.as_ref())
    }

    fn push(&self, lock: Self::Lock) -> Result<(), WriteError<Self::Lock, Self::Error>> {
        <S as Write<T>>::push(self.as_ref(), lock)
    }

    fn abort(&self, lock: Self::Lock) -> Result<(), WriteError<Self::Lock, Self::Error>> {
        <S as Write<T>>::abort(self.as_ref(), lock)
    }
}

impl<S> Validate for Arc<S>
where
    S: Validate
{
    type Item = S::Item;
    type Error = S::Error;
    type Version = S::Version;

    fn validate(
        &self,
        version: Self::Version
    ) -> Result<Validation<Self::Version, Self::Item>, Self::Error> {
        <S as Validate>::validate(self.as_ref(), version)
    }

    fn refresh(&self) -> Result<(Self::Version, Option<Self::Item>), Self::Error> {
        <S as Validate>::refresh(self.as_ref())
    }
}

pub trait ReadWrite
where
    Self: Read + Write<<Self as Validate>::Item, Error = <Self as Validate>::Error> {}

impl<T> ReadWrite for T
where
    Self: Read + Write<<Self as Validate>::Item, Error = <Self as Validate>::Error>
{}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Constant<V, T> {
        version: V,
        value: T,
        num_hits: Arc<AtomicUsize>
    }

    impl<V, T> Validate for Constant<V, T>
    where
        V: Clone,
        T: Clone
    {
        type Item = T;
        type Error = ();
        type Version = V;

        fn validate(&self, v: V) -> Result<Validation<V, T>, ()> {
            self.num_hits.fetch_add(1, Ordering::Relaxed);
            Ok(Validation {
                version: self.version.clone(),
                update: ContentUpdate::Value(self.value.clone())
            })
        }

        fn refresh(&self) -> Result<(V, Option<T>), ()> {
            self.num_hits.fetch_add(1, Ordering::Relaxed);
            Ok((self.version.clone(), Some(self.value.clone())))
        }
    }

    impl<V, T> Constant<V, T> {
        fn new(version: V, value: T) -> Self {
            Self { version, value, num_hits: Arc::new(AtomicUsize::new(0)) }
        }
    }

    #[test]
    fn time_to_live() {
        let constant = Constant::new(0, 0);
        let num_hits = constant.num_hits.clone();
        let cached = constant
            .map(|value| value + 1)
            .time_to_live(Duration::from_millis(5))
            .into_cached()
            .unwrap();
        for _ in 1..10 {
            cached.read_with(|_| ()).unwrap().unwrap();
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(num_hits.swap(0, Ordering::Relaxed), 2)
    }
}
