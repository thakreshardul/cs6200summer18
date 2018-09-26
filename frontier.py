import heapq


class Frontier:
    def __init__(self):
        self.heap = []
        self.in_links = dict()

    def insert(self, value, parent=None):
        if value in self.heap:
            index = self.heap.index(value)
            heap_item = self.heap.pop(index)
            try:
                self.in_links[heap_item.url].append(parent)
            except KeyError:
                self.in_links[heap_item.url] = [parent, ]
            heap_item.priority -= 1
            heapq.heappush(self.heap, heap_item)
        else:
            self.in_links[value.url] = [parent, ]
            heapq.heappush(self.heap, value)

    def pop(self):
        return heapq.heappop(self.heap)

    # def heapify(self):
    #     heapq.heapify(self.heap)

    def size(self):
        return len(self.heap)

    def print_frontier(self):
        for x in self.heap:
            print(x.url)
