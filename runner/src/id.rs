/// Defines an id newtype for an entity stored in a DB table with a
/// `SERIAL PRIMARY KEY CHECK (id <> 0)` column, so the id is never zero.
macro_rules! define_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
        pub struct $name(std::num::NonZeroU32);

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl TryFrom<u32> for $name {
            type Error = ();
            fn try_from(v: u32) -> Result<Self, Self::Error> {
                std::num::NonZeroU32::new(v).map($name).ok_or(())
            }
        }

        impl TryFrom<i32> for $name {
            type Error = ();
            fn try_from(v: i32) -> Result<Self, Self::Error> {
                u32::try_from(v).map_err(|_| ()).and_then($name::try_from)
            }
        }

        impl $name {
            /// Converts an id read from the DB, where the column's `CHECK (id <> 0)` makes it
            /// valid; panics otherwise.
            pub(crate) fn from_db(v: i32) -> Self {
                $name::try_from(v).unwrap_or_else(|_| {
                    panic!("invalid {} {v} loaded from db", stringify!($name))
                })
            }

            #[inline]
            pub fn as_u32(self) -> u32 {
                self.0.get()
            }

            #[inline]
            pub fn as_i32(self) -> i32 {
                self.0.get() as i32
            }
        }
    };
}

pub(crate) use define_id;
