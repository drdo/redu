use std::{
    cmp::max,
    collections::{hash_map, HashMap},
    iter::Peekable,
};

use thiserror::Error;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SizeTree(pub FileTree<usize>);

#[derive(Debug, Eq, Error, PartialEq)]
pub enum InsertError {
    #[error("Tried to insert into empty path")]
    EmptyPath,
    #[error("Tried to insert into existing path")]
    EntryExists,
}

impl SizeTree {
    pub fn new() -> Self {
        SizeTree(FileTree::new())
    }

    pub fn merge(self, other: SizeTree) -> Self {
        SizeTree(self.0.merge(other.0, max))
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (usize, &str, usize, bool)> + '_ {
        self.0
            .iter()
            .map(|(level, cs, size, is_dir)| (level, cs, *size, is_dir))
    }

    // `update` is used to update the sizes for all ancestors
    pub fn insert<C, P>(
        &mut self,
        path: P,
        size: usize,
    ) -> Result<(), InsertError>
    where
        C: AsRef<str>,
        P: IntoIterator<Item = C>,
    {
        let (mut breadcrumbs, mut remaining) = {
            let (breadcrumbs, remaining) = self.0.find(path);
            (breadcrumbs, remaining.peekable())
        };
        if remaining.peek().is_none() {
            return Err(InsertError::EntryExists);
        }

        // Update existing ancestors
        for node in breadcrumbs.iter_mut() {
            unsafe { (**node).data += size };
        }

        // Create the rest
        let mut current_node: &mut Node<usize> = {
            if let Some(last) = breadcrumbs.pop() {
                unsafe { &mut *last }
            } else if let Some(component) = remaining.next() {
                self.0
                    .children
                    .entry(Box::from(component.as_ref()))
                    .or_insert(Node::new(size))
            } else {
                return Err(InsertError::EmptyPath);
            }
        };
        for component in remaining {
            current_node = current_node
                .children
                .entry(Box::from(component.as_ref()))
                .or_insert(Node::new(0));
            current_node.data = size;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FileTree<T> {
    children: HashMap<Box<str>, Node<T>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Node<T> {
    data: T,
    children: HashMap<Box<str>, Node<T>>,
}

impl<T> FileTree<T> {
    pub fn new() -> Self {
        FileTree { children: HashMap::new() }
    }

    pub fn merge<F>(self, other: Self, mut combine: F) -> Self
    where
        F: FnMut(T, T) -> T,
    {
        fn merge_children<T, F: FnMut(T, T) -> T>(
            a: HashMap<Box<str>, Node<T>>,
            b: HashMap<Box<str>, Node<T>>,
            f: &mut F,
        ) -> HashMap<Box<str>, Node<T>> {
            let mut sorted_a = sorted_hashmap(a).into_iter();
            let mut sorted_b = sorted_hashmap(b).into_iter();
            let mut children = HashMap::new();
            loop {
                match (sorted_a.next(), sorted_b.next()) {
                    (Some((name0, tree0)), Some((name1, tree1))) => {
                        if name0 == name1 {
                            children.insert(name0, merge_node(tree0, tree1, f));
                        } else {
                            children.insert(name0, tree0);
                            children.insert(name1, tree1);
                        }
                    }
                    (None, Some((name, tree))) => {
                        children.insert(name, tree);
                    }
                    (Some((name, tree)), None) => {
                        children.insert(name, tree);
                    }
                    (None, None) => {
                        break;
                    }
                }
            }
            children
        }

        // This exists to be able to reuse `combine` multiple times in the loop
        // without being consumed by the recursive calls
        fn merge_node<T, F: FnMut(T, T) -> T>(
            a: Node<T>,
            b: Node<T>,
            f: &mut F,
        ) -> Node<T> {
            Node {
                data: f(a.data, b.data),
                children: merge_children(a.children, b.children, f),
            }
        }

        FileTree {
            children: merge_children(
                self.children,
                other.children,
                &mut combine,
            ),
        }
    }

    /// Depth first, parent before children
    pub fn iter(&self) -> Iter<'_, T> {
        let breadcrumb =
            Breadcrumb { level: 1, children: self.children.iter() };
        Iter { stack: vec![breadcrumb] }
    }

    /// Traverse the tree while keeping a context.
    /// The context is morally `[f(node_0), f(node_1), ..., f(node_2)]` for
    /// all ancestors nodes `node_i` of the visited node.
    ///
    /// Depth first, parent before children
    pub fn traverse_with_context<'a, C, E, F>(
        &'a self,
        mut f: F,
    ) -> Result<(), E>
    where
        F: for<'b> FnMut(&'b [C], &'a str, &'a T, bool) -> Result<C, E>,
    {
        let mut iter = self.iter();
        // First iteration just to initialized id_stack and previous_level
        let (mut context, mut previous_level): (Vec<C>, usize) = {
            if let Some((level, component, data, is_dir)) = iter.next() {
                let context_component = f(&[], component, data, is_dir)?;
                (vec![context_component], level)
            } else {
                return Ok(());
            }
        };

        for (level, component, size, is_dir) in iter {
            if level <= previous_level {
                // We went up the tree or moved to a sibling
                for _ in 0..previous_level - level + 1 {
                    context.pop();
                }
            }
            context.push(f(&context, component, size, is_dir)?);
            previous_level = level;
        }
        Ok(())
    }

    /// Returns the breadcrumbs of the largest prefix of the path.
    /// If the file is in the tree the last breadcrumb will be the file itself.
    /// Does not modify self at all.
    /// The cdr is the remaining path that did not match, if any.
    fn find<C, P>(
        &mut self,
        path: P,
    ) -> (Vec<*mut Node<T>>, impl Iterator<Item = C>)
    where
        C: AsRef<str>,
        P: IntoIterator<Item = C>,
    {
        let mut iter = path.into_iter().peekable();
        if let Some(component) = iter.peek() {
            let component = component.as_ref();
            if let Some(node) = self.children.get_mut(component) {
                iter.next();
                return node.find(iter);
            }
        }
        (vec![], iter)
    }
}

impl<T> Node<T> {
    fn new(data: T) -> Self {
        Node { data, children: HashMap::new() }
    }

    fn find<C, P>(
        &mut self,
        mut path: Peekable<P>,
    ) -> (Vec<*mut Node<T>>, Peekable<P>)
    where
        C: AsRef<str>,
        P: Iterator<Item = C>,
    {
        let mut breadcrumbs: Vec<*mut Node<T>> = vec![self];
        while let Some(c) = path.peek() {
            let c = c.as_ref();
            let current = unsafe { &mut **breadcrumbs.last().unwrap() };
            match current.children.get_mut(c) {
                Some(next) => {
                    breadcrumbs.push(next);
                    path.next();
                }
                None => break,
            }
        }
        (breadcrumbs, path)
    }
}

pub struct Iter<'a, T> {
    stack: Vec<Breadcrumb<'a, T>>,
}

struct Breadcrumb<'a, T> {
    level: usize,
    children: hash_map::Iter<'a, Box<str>, Node<T>>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    /// (level, component, data, is_directory)
    type Item = (usize, &'a str, &'a T, bool);

    /// Depth first, parent before children
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(mut breadcrumb) = self.stack.pop() {
                if let Some((component, child)) = breadcrumb.children.next() {
                    let level = breadcrumb.level + 1;
                    let item = (
                        level,
                        component as &str,
                        &child.data,
                        !child.children.is_empty(),
                    );
                    self.stack.push(breadcrumb);
                    self.stack.push(Breadcrumb {
                        level,
                        children: child.children.iter(),
                    });
                    break Some(item);
                }
            } else {
                break None;
            }
        }
    }
}

fn sorted_hashmap<K: Ord, V>(m: HashMap<K, V>) -> Vec<(K, V)> {
    let mut vec = m.into_iter().collect::<Vec<_>>();
    vec.sort_unstable_by(|(k0, _), (k1, _)| k0.cmp(k1));
    vec
}
