from collections import MutableMapping
from functools import total_ordering


def set_siblings_pair(left, right):
    left.next_sibling = right
    right.prev_sibling = left


def set_siblings(children):
    for i in range(1, len(children)):
        set_siblings_pair(children[i - 1], children[i])


# https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html
# http://www.cburch.com/cs/340/reading/btree/
# B+-tree maintains the following invariants:
# * Every node has one more references than it has keys.
# * All leaves are at the same distance from the root.
# * For every non-leaf node N with k being the number of keys in N: all keys in the first child's subtree are less than N's first key; and all keys in the ith child's subtree (2 ≤ i ≤ k) are between the (i − 1)th key of n and the ith key of n.
# * The root has at least two children.
# * Every non-leaf, non-root node has at least floor(d / 2) children.
# * Each leaf contains at least floor(d / 2) keys.
# * Every key from the table appears in a leaf, in left-to-right sorted order.


@total_ordering
class Node:
    def has_key_space(self):
        return len(self.keys) < self.degree - 1

    def has_value_space(self):
        return len(self.keys) < self.degree

    def __lt__(self, other):
        return self.keys[-1] < other.keys[0]

    def print(self, level):
        strs = []
        pad = ''.ljust(level * 3, ' ')
        strs.append('{}{}:{}={}'.format(pad, level, type(self).__name__, self.keys))
        for child in self.children:
            strs.extend(child.print(level + 1))
        return strs


class InteriorNode(Node):
    def __init__(self, degree, parent=None):
        self.degree = degree
        self.parent = parent
        self.keys = []
        self.children = []

    def search(self, key):
        for i, k in enumerate(self.keys):
            if key < k:
                return self.children[i].search(key)
        return self.children[-1].search(key)

    def search_for_node(self, key):
        for i, k in enumerate(self.keys):
            if key < k:
                return self.children[i].search_for_node(key)
        return self.children[-1].search_for_node(key)

    def insert(self, key, value):
        child = None
        index = 0
        for i, k in enumerate(self.keys):
            if key < k:
                child = self.children[i]
                index = i
                break
        if child is None:
            child = self.children[-1]
            index = len(self.children) - 1

        split_key, result = child.insert(key, value)
        self.children[index:index + 1] = result
        if index > 0:
            set_siblings_pair(self.children[index - 1], self.children[index])
        if index < len(self.children) - 2:
            set_siblings_pair(self.children[index + 1], self.children[index + 2])

        if len(result) > 1:
            # Two new children
            self.keys[index:index] = [split_key]
        if not self.has_value_space():
            left = InteriorNode(self.degree, self.parent)
            right = InteriorNode(self.degree, self.parent)
            new_keys = self.keys
            half = len(new_keys) // 2
            left.keys = new_keys[:half]
            left.children = self.children[:half + 1]
            right.keys = new_keys[half + 1:]
            right.children = self.children[half + 1:]
            return new_keys[half], [left, right]

        return None, [self]

    def delete(self, key):
        for i, k in enumerate(self.keys):
            if key < k:
                child = self.children[i]
                index = i
                break
        if child is None:
            child = self.children[-1]
            index = len(self.children) - 1

        new_child = child.delete(key)
        self.children[i] = new_child
        # if len(self.children[i].keys)==0:

    def __repr__(self):
        return '{}: {}'.format(type(self).__name__, self.keys)


class LeafNode(Node):
    def __init__(self, degree, parent=None, prev_sibling=None, next_sibling=None):
        self.degree = degree
        self.parent = parent
        self.prev_sibling = prev_sibling
        self.next_sibling = next_sibling
        self.keys = []
        self.values = []
        self.children = []

    def search(self, key):
        for i, k in enumerate(self.keys):
            if k == key:
                return self.values[i]
        raise KeyError

    def search_for_node(self, key):
        return self

    def _set_key_value(self, key, value):
        for i, k in enumerate(self.keys):
            if k == key:
                self.values[i] = value
                return True
        return False

    def insert(self, key, value):
        if self._set_key_value(key, value):
            # Replacing existing key-value
            return None, [self]

        if self.has_key_space():
            # Leaf node is not full
            for i, k in enumerate(self.keys):
                if key < k:
                    self.keys[i:i] = [key]
                    self.values[i:i] = [value]
                    return None, [self]
            self.keys.append(key)
            self.values.append(value)
            return None, [self]
        else:
            found = False
            for i, k in enumerate(self.keys):
                if key < k:
                    self.keys[i:i] = [key]
                    self.values[i:i] = [value]
                    found = True
                    break
            if not found:
                self.keys.append(key)
                self.values.append(value)
            left = LeafNode(self.degree, self.parent)
            right = LeafNode(self.degree, self.parent)
            set_siblings_pair(left, right)
            half = len(self.keys) // 2
            left.keys = self.keys[:half]
            left.values = self.values[:half]
            right.keys = self.keys[half:]
            right.values = self.values[half:]
            return right.keys[0], [left, right]

    def delete(self, key):
        for i, k in enumerate(self.keys):
            if k == key:
                del self.keys[i]
                del self.values[i]
                break
        return self

    def __repr__(self):
        return '{}: {}'.format(type(self).__name__, self.keys)


class BTree(MutableMapping):
    def __init__(self, degree=4):
        self.root = LeafNode(degree, None)

    def __len__(self):
        node = self.root
        size = 0
        while type(node) != LeafNode:
            node = node.children[0]
        while node is not None:
            size += len(node.keys)
            node = node.next_sibling
        return size

    def _slice(self, sp):
        if sp.step is not None:
            raise Exception('Cannot slice with step')
        start = sp.start
        end = sp.stop
        if start is not None:
            node = self.root.search_for_node(start)
            while node is not None:
                for i, k in enumerate(node.keys):
                    if k >= start:
                        if end is not None and k < end:
                            yield node.values[i]
                        elif end is not None:
                            break
                        else:
                            yield node.values[i]
                node = node.next_sibling
        elif end is not None:
            node = self.root.search_for_node(end)
            while node is not None:
                for i, k in enumerate(reversed(node.keys)):
                    if k < end:
                        index = len(node.keys) - 1 - i
                        yield node.values[index]
                node = node.prev_sibling
        else:
            node = self.root
            while type(node) != LeafNode:
                node = node.children[0]
            while node is not None:
                for v in node.values:
                    yield v
                node = node.next_sibling

    def __getitem__(self, key):
        if isinstance(key, slice):
            return self._slice(key)
        return self.root.search(key)

    def __contains__(self, item):
        try:
            self.__getitem__(item)
            return True
        except KeyError:
            return False

    def search(self, key):
        try:
            return self.__getitem__(key)
        except KeyError:
            return None

    def __setitem__(self, key, value):
        split_key, r = self.root.insert(key, value)
        if len(r) > 1:
            self.root = InteriorNode(self.root.degree)
            self.root.children = r
            self.root.keys = [split_key]
        else:
            self.root = r[0]

    def __delitem__(self, key):
        pass

    def __iter__(self):
        node = self.root
        while type(node) != LeafNode:
            node = node.children[0]
        while node is not None:
            for k in node.keys:
                yield k
            node = node.next_sibling

    def __reversed__(self):
        node = self.root
        while type(node) != LeafNode:
            node = node.children[-1]
        while node is not None:
            for k in reversed(node.keys):
                yield k
            node = node.prev_sibling

    def __repr__(self):
        return '\n'.join(self.root.print(0))


def insert(tree, key):
    tree[key] = 'v{}'.format(key)
    print(tree)
    print('Size={}'.format(len(tree)))
    print()


if __name__ == '__main__':
    tree = BTree()

    print(len(tree))
    insert(tree, 4)
    insert(tree, 6)
    insert(tree, 8)
    insert(tree, 10)
    insert(tree, 9)
    insert(tree, 11)
    insert(tree, 5)
    insert(tree, 7)
    insert(tree, 12)
    insert(tree, 13)
    insert(tree, 3)
    insert(tree, 2)
    insert(tree, 1)
    insert(tree, 0)
    insert(tree, 0)
    for i in range(14, 26):
        insert(tree, i)

    print(list(reversed(tree)))
    print(tree[12])
    print(12 in tree)
    print(100 in tree)
    print(tree[4])
    tree[4] = 17
    print(tree[4])

    print('Slice')
    print(list(tree[3:9]))
    print(list(tree[3:9]))
