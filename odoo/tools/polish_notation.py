class Node(object):
    data = None

    def __init__(self, left, operator=None, right=None):
        if operator:
            self.data = [operator]
        else:
            self.data = []
        for item in (left, right):
            if isinstance(item, Node):
                self.data.extend(item.data)
            elif item is not None:
                self.data.append(item)


    def __eq__(self, other):
        if isinstance(other, Node):
            return self.data == other.data
        else:
            return False


    @classmethod
    def create_node(cls, triplet, inspect=None):
        try:
            operator = triplet[0]
            left = triplet[1]
        except IndexError:
            return None

        try:
            right = triplet[2]
        except IndexError:
            right = None

        if inspect:
            if isinstance(left, (tuple, list)):
                left = inspect(left)
            if isinstance(right, (tuple, list)):
                right = inspect(right)

        if left is None and right is None:
            return None

        elif left is None:
            return cls(right)

        elif right is None:
            return cls(left)

        elif left == right:
            return cls(left)

        return cls(left, operator, right)


def replace(domain, callback=None):
    tail = idx = len(domain)
    while idx > 0:
        idx -= 1
        if not isinstance(domain[idx], (tuple, list, Node)):
            node = Node.create_node(domain[idx:idx + 3], callback)
            tail = idx + 1
        elif tail - idx == 3:
            idx += 1
            node = Node.create_node(['&'] + domain[idx:idx + 2], callback)
            tail = idx + 1
        elif idx == 0:
            node = Node.create_node(['&'] + domain[idx:idx + 2], callback)
        else:
            continue

        if idx:
            domain = domain[:idx] + [node] + domain[idx + 3:]

        # This was last item in the list.
        elif node is None:
            domain = []
        else:
            domain = node.data

    return domain


def strip(domain, exclude_fields=[]):
    def _inspect_domain(item):
        if item[0] in exclude_fields:
            return None
        return item

    return replace(domain, _inspect_domain)
