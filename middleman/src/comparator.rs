/// Implements Ord/PartialOrd for a struct given the list of fields to compare
/// in lexicographic order. Using Ord as the comparator for a database key is
/// optional but makes variable-length keys in particular easier to work with.
#[macro_export]
macro_rules! make_comparator {
    ($Name:ident, $($field:ident),*$(,)?) => {
        impl std::cmp::Ord for $Name {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                std::cmp::Ord::cmp(
                    ($(self.$field(),)*),
                    ($(other.$field(),)*),
                )
            }
        }

        impl std::cmp::PartialOrd for $Name {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(std::cmp::Ord::cmp(self, other))
            }
        }
    };
}
