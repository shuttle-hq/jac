use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{Mutex, RwLock, Arc};
use std::time::{Duration, Instant};

/// A wrapper around content returned as part of an invalidation
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

    type Error: std::error::Error;

    type Version;

    /// `validate` content at `version`. This should be quick to do and only rarely return
    /// new content.
    fn validate(
        &self,
        version: Self::Version
    ) -> Result<Validation<Self::Version, Self::Item>, Self::Error>;

    /// get content with its current version
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
                strategy: Arc::new(self),
                version: Arc::new(RwLock::new(version)),
                content: Content(Arc::new(RwLock::new(data)))
            }
        )
    }
}

pub struct TimeToLive<S: Validate> {
    inner: S,
    ttl: Duration,
    last: Mutex<Option<S::Version>>
}

impl<S> Validate for TimeToLive<S>
where
    S: Validate
{
    type Item = S::Item;

    type Error = S::Error;

    type Version = Instant;

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
    F: Fn(T::Item) -> O,
{
    type Item = O;
    type Error = T::Error;
    type Version = T::Version;
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

    fn refresh(&self) -> Result<(Self::Version, Option<Self::Item>), Self::Error> {
        let (version, value) = self.inner.refresh()?;
        Ok((version, value.map(|v| (self.closure)(v))))
    }
}

pub struct Content<C>(Arc<RwLock<Option<C>>>);

impl<C> Clone for Content<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// A wrapper around a validation strategy that caches its content
pub struct Cached<S: Validate> {
    strategy: Arc<S>,
    version: Arc<RwLock<S::Version>>,
    content: Content<S::Item>,
}

impl<S: Validate> Clone for Cached<S> {
    fn clone(&self) -> Self {
        Self {
            strategy: self.strategy.clone(),
            version: self.version.clone(),
            content: self.content.clone()
        }
    }
}

#[derive(Clone, Debug)]
pub enum CacheError<E> {
    Backend(E),
    Poisoned
}

impl<T, E> From<std::sync::PoisonError<T>> for CacheError<E> {
    fn from(pe: std::sync::PoisonError<T>) -> Self {
        Self::Poisoned
    }
}

impl<E> std::fmt::Display for CacheError<E>
where
    E: std::error::Error
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Backend(e) => {
                write!(f, "backend: {}", e)
            },
            Poisoned => {
                write!(f, "poisoned")
            }
        }
    }
}

impl<E> std::error::Error for CacheError<E> where E: std::error::Error {}

impl<S> Cached<S>
where
    S: Validate,
    S::Version: Clone
{
    fn validate(&self) -> Result<(), CacheError<S::Error>> {
        let version = (*self.version.read()?).clone();

        let res = self.strategy.validate(version)
            .map_err(|e| CacheError::Backend(e))?;
        
        let version = res.version;

        match res.update {
            ContentUpdate::Unchanged => {},
            ContentUpdate::Removed => {
                let mut c_w = self.content.0.write()?;
                let mut v_w = self.version.write()?;
                *v_w = version;
                *c_w = None;
            },
            ContentUpdate::Value(v) => {
                let mut c_w = self.content.0.write()?;
                let mut v_w = self.version.write()?;
                *v_w = version;
                *c_w = Some(v);
            }
        }

        Ok(())
    }
}

pub trait Read {
    type Item;

    type Error: std::error::Error;

    fn try_read_with<O, F>(&self, f: F) -> Result<Option<O>, CacheError<Self::Error>>
    where
        F: FnOnce(&Self::Item) -> O;
}

impl<S> Read for Cached<S>
where
    S: Validate,
    S::Version: Clone
{
    type Item = S::Item;

    type Error = S::Error;

    fn try_read_with<O, F>(&self, f: F) -> Result<Option<O>, CacheError<S::Error>>
    where
        F: FnOnce(&S::Item) -> O
    {
        self.validate()?;
        self.content
            .0
            .read()
            .map_err(|_| CacheError::Poisoned)
            .map(|o| o.as_ref().map(f))
    }
}

pub trait OwnedIntent<T> {
    type Op;
    fn apply(self) -> Self::Op;
    fn discard(self) -> Self::Op;
}

pub trait Write<T>: Sized {
    type Error;

    type Intent: OwnedIntent<T>;

    fn try_write(&self) -> Result<Self::Intent, Self::Error>;

    fn apply(
        &self,
        op: <Self::Intent as OwnedIntent<T>>::Op
    ) -> Result<(), Self::Error>;

    fn try_write_with<F>(&self, f: F) -> Result<(), Self::Error>
    where
        F: FnOnce(Self::Intent) -> <Self::Intent as OwnedIntent<T>>::Op
    {
        self.try_write().map(f).and_then(|intent| self.apply(intent))
    }
}

impl<T, I, F, O> Write<T> for ValidateMap<I, F, O>
where
    I: Write<T>
{
    type Error = I::Error;

    type Intent = I::Intent;

    fn try_write(&self) -> Result<Self::Intent, Self::Error> {
        self.inner.try_write()
    }

    fn apply(
        &self,
        op: <Self::Intent as OwnedIntent<T>>::Op
    ) -> Result<(), Self::Error> {
        self.inner.apply(op)
    }
}

impl<I, S> Write<I> for Cached<S>
where
    S: Write<I> + Validate
{
    type Error = <S as Write<I>>::Error;

    type Intent = S::Intent;

    fn try_write(&self) -> Result<Self::Intent, Self::Error> {
        self.strategy.try_write()
    }

    fn apply(
        &self,
        op: <Self::Intent as OwnedIntent<I>>::Op
    ) -> Result<(), Self::Error> {
        self.strategy.apply(op)
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
            cached.try_read_with(|_| ()).unwrap().unwrap();
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(num_hits.swap(0, Ordering::Relaxed), 2)
    }
}
