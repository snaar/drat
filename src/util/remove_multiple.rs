pub trait RemoveMultiple {
    /// Removes elements at positions specified by `indices`,
    /// shifting all elements after it to the left.
    ///
    /// `indices` is assumed to be sorted in an ascending order
    fn remove_multiple(&mut self, indices: &[usize]);
}

impl<T> RemoveMultiple for Vec<T> {
    fn remove_multiple(&mut self, indices: &[usize]) {
        if indices.is_empty() {
            return;
        }

        let mut prev_index = indices[0];
        let mut removed_count = 1;
        let slice: &mut [T] = self.as_mut();

        for &current_index in &indices[1..] {
            slice[prev_index - (removed_count - 1)..current_index].rotate_left(removed_count);
            prev_index = current_index;
            removed_count += 1;
        }

        slice[prev_index - (removed_count - 1)..].rotate_left(removed_count);
        self.truncate(self.len() - removed_count);
    }
}

#[cfg(test)]
mod tests {
    use crate::util::remove_multiple::RemoveMultiple;

    #[test]
    fn test_remove_multiple() {
        let mut vec = vec![1, 2, 3, 4, 5];
        vec.remove_multiple(&vec![]);
        assert_eq!(vec, vec![1, 2, 3, 4, 5]);

        let mut vec = vec![1, 2, 3, 4, 5];
        vec.remove_multiple(&vec![0, 1, 2, 3, 4]);
        assert_eq!(vec, Vec::new() as Vec<i32>);

        let mut vec = vec![1, 2, 3, 4, 5];
        vec.remove_multiple(&vec![0]);
        assert_eq!(vec, vec![2, 3, 4, 5]);

        let mut vec = vec![1, 2, 3, 4, 5];
        vec.remove_multiple(&vec![4]);
        assert_eq!(vec, vec![1, 2, 3, 4]);

        let mut vec = vec![1, 2, 3, 4, 5];
        vec.remove_multiple(&vec![0, 4]);
        assert_eq!(vec, vec![2, 3, 4]);

        let mut vec = vec![1, 2, 3, 4, 5];
        vec.remove_multiple(&vec![1, 2, 3]);
        assert_eq!(vec, vec![1, 5]);

        let mut vec = vec![1, 2, 3, 4, 5];
        vec.remove_multiple(&vec![2]);
        assert_eq!(vec, vec![1, 2, 4, 5]);
    }
}
