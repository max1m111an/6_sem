from dataclasses import dataclass
from typing import Optional
import pandas as pd

Data = int  # type for node names

@dataclass
class Node:
    id: Data
    edges: dict[Data, int]

    def __init__(self, node_id: Data) -> None:
        self.id = node_id
        self.edges = {}


class Graph:
    _table: list[Node]

    def __init__(self) -> None:
        self._table = []

    def __len__(self) -> int:
        return len(self._table)

    def is_empty(self) -> bool:
        return len(self) == 0

    def contains_by_node(self, item: Node) -> bool:
        if self.is_empty():
            return False
        return item in self._table

    def contains_by_id(self, index: Data) -> bool:
        if self.is_empty():
            return False
        for i in range(len(self)):
            if index == self._table[i].id:
                return True
        return False

    def add_node(self, node: Node) -> None:
        if self.contains_by_node(node):
            print("Node with this id is already existed.")
        self._table.append(node)
        self._table.sort(key=lambda node: node.id)

    def add_edge(self, first: Node, second: Node, dist: int, non_oriented: bool = False) -> None:
        if not (self.contains_by_node(first) and self.contains_by_node(second)):
            print("Unknown node.")
            return
        if first.id == second.id:
            return
        first.edges[second.id] = dist
        if non_oriented:
            second.edges[first.id] = dist

    def find_node(self, index: Data) -> Optional[Node]:
        for node in self._table:
            if node.id == index:
                return node
        return None
    
    def get_node(self, node_num: Data) -> Node:
        node: Optional[Node] = self.find_node(node_num)
        if not node:
            node = Node(node_id=node_num)
            self.add_node(node)
        return node

    def put(self, one: Data, two: Data, dist: int, non_oriented=False) -> None:
        if one == two:
            return
        first: Node = self.get_node(one)
        second: Node = self.get_node(two)

        self.add_edge(first, second, dist, non_oriented)

    def get_dist_neighbors(self, one: Data, two: Data) -> int:
        if not (self.contains_by_id(one) and self.contains_by_id(two)) or one == two \
                or self.find_node(one).edges.get(two) is None:
            return 0

        first: Optional[Node] = self.find_node(one)
        return first.edges[two]

    @staticmethod
    def load_matrix(mat: list[list[int]], vertex_list: list[Data] = []) -> 'Graph':
        gr: Graph = Graph()
        length: int = len(mat[0])
        for i in range(length):
            for j in range(length):
                if mat[i][j] > 0:
                    gr.put(i + 1, j + 1, mat[i][j]) if len(vertex_list) == 0 else \
                    gr.put(vertex_list[i], vertex_list[j], mat[i][j])
        gr._table.sort(key=lambda node: node.id)
        return gr

    def __str__(self) -> str:
        if self.is_empty():
            return "<empty graph>"
        
        n = len(self)

        mat: list[list[str | Data]] = [["0"] * n for _ in range(n)]
        all_nodes: list[Data] = [cols.id for cols in self._table]
        max_digits: int = max([len(str(self.get_dist_neighbors(all_nodes[i], all_nodes[j]))) for i in range(n) for j in range(n)])

        ss: str = "Graph:\n  _|" + "_" * (max_digits - 1)
        all_nodes.sort()

        for i in range(n):
            for j in range(n):
                dist: int = self.get_dist_neighbors(all_nodes[i], all_nodes[j])
                mat[i][j] = " " * (max_digits - len(str(dist))) + str(dist)

        for a in all_nodes:
            ss += f"{a}|{"_" * (max_digits - 1)}"
        ss += "\n"

        for i in range(n):
            ss += f"|{all_nodes[i]}| "
            for j in range(n):
                ss += f"{mat[i][j]}|"
            ss += "\n"  # + "_" * len(all_nodes) * 3 + "\n"
        return ss

    def get_matrix_vertex_degree(self) -> list[list[int]]:
        if self.is_empty():
            return []
        
        n = len(self)

        result_mat: list[list[int]] = [[0] * n for _ in range(n)]
        vertex_degree_list: list[int] = [0 for _ in range(n)]

        for i in range(n):
            for vertex, dist in self._table[i].edges.items():
                vertex_degree_list[i] += dist
                vertex_degree_list[vertex - 1] += dist
        
        for i in range(n):
            result_mat[i][i] = vertex_degree_list[i]

        return result_mat
    
    def get_matrix_incident(self) -> list[list[int]]:
        if self.is_empty():
            return []
        
        sorted_nodes = sorted(self._table, key=lambda x: x.id)
        node_index = {node.id: i for i, node in enumerate(sorted_nodes)}
        
        edges = []
        for node in sorted_nodes:
            for neighbor, weight in sorted(node.edges.items()):
                edges.append((node.id, neighbor, weight))
        
        edges.sort(key=lambda x: (x[0], x[1]))  # edge sort by vertex id
        
        result_mat = [[0] * len(edges) for _ in range(len(sorted_nodes))]
        
        for col, (v1, v2, weight) in enumerate(edges):
            result_mat[node_index[v1]][col] = weight
            result_mat[node_index[v2]][col] = -weight
        
        return result_mat

    @staticmethod
    def logic_arr_addition(arr1: list[int], arr2: list[int]) -> list[int]:
        # non-oriented weightless graph only
        if len(arr1) != len(arr2):
            return []
        result: list[int] = []
        for a, b in zip(arr1, arr2):
            result.append(1 if a == 1 or b == 1 else 0)
        return result


    def get_matrix_reachability(self) -> list[list[int]]:
        if self.is_empty():
            return []
        
        n = len(self)

        result_mat: list[list[int]] = [[0] * n for _ in range(n)]
        reaches_dict: dict[int, list[int]] = {
            x + 1: [0] * n for x in range(n)
        }

        for i in range(n, 0, -1):
            for v in self._table[i - 1].edges.keys():
                reaches_dict[i][v - 1] = 1  # all neighbours
                if i != n:
                    # indirect nighbours
                    reaches_dict[i] = Graph.logic_arr_addition(reaches_dict[i], reaches_dict[v])
            reaches_dict[i][i - 1] = 1  # itself
            
        
        for id, v in reaches_dict.items():
            result_mat[id - 1] = v

        return result_mat

    def get_matrix_dist(self) -> list[list[int]]:
        if self.is_empty():
            return []
        
        n = len(self)
        INF = float('inf')
        result_mat: list[list[int]] = [[INF] * n for _ in range(n)]
    
        for i in range(n):
            result_mat[i][i] = 0

        for i in range(n):
            for v, d in self._table[i].edges.items():
                result_mat[i][v - 1] = d  # adjacency mat view
    
        # WFI algo
        for k in range(n):  # intermed
            for i in range(n):  # start vertex
                for j in range(n):  # end vertex
                    # dist(i,k) + dist(k,j) ? dist(i,j) - triangle inequality
                    if result_mat[i][k] + result_mat[k][j] < result_mat[i][j]:
                        result_mat[i][j] = result_mat[i][k] + result_mat[k][j]
    
        return result_mat
    
    def get_matrix_kirchhoff(self) -> list[list[int]]:
        if self.is_empty():
            return []
        
        # diagonal values from vertex degree mat
        result_mat: list[list[int]] = self.get_matrix_vertex_degree()

        # symmetrical weights for vertexes
        for i in range(len(self)):
            for v, d in self._table[i].edges.items():
                result_mat[v - 1][i] = -d
                result_mat[i][v - 1] = -d

        return result_mat
    
    def retraction_vertexes(self, first_num: Data, second_num: Data) -> 'Graph':
        graph: Graph = self
        
        # weightless
        graph._table[first_num].edges.update(graph._table[second_num].edges)
        del graph._table[second_num - 1]
        
        graph._table.sort(key=lambda node: node.id)
        return graph
    
    def concat_graphs_by_node(self, graph: 'Graph', node_id: Data) -> 'Graph':
        # non-oriented weightless graph only
        result: Graph = self

        # adding 'graph' nodes
        for node in graph._table:
            result.get_node(node.id)
        
        # adding 'graph' edges
        for node in graph._table:
            for neighbor_id, w in node.edges.items():
                if result.contains_by_id(node.id) and result.contains_by_id(neighbor_id):
                    result.put(node.id, neighbor_id, w)
    
        result.put(node_id, graph._table[0].id, 1, True)
        return result
    
    def get_edges_list_str(self) -> list[str]:
        edges = []
    
        for node in self._table:
            for neighbor_id in node.edges.keys():
                if node.id < neighbor_id:
                    edges.append((node.id, neighbor_id))
        
        edges.sort(key=lambda x: (x[0], x[1]))
        edges = [f"{x1}{x2}" for x1, x2 in edges]
        return edges    


if __name__ == "__main__":
    print('Задание 1')

    matrix: list[list[int]] = [[0, 2, 4, 0, 0, 0],
                               [0, 0, 0, 7, 0, 0],
                               [0, 0, 0, 1, 0, 15],
                               [0, 0, 0, 0, 3, 0],
                               [0, 0, 0, 0, 0, 2],
                               [0, 0, 0, 0, 0, 0]]

    graph1: Graph = Graph.load_matrix(matrix)
    print(graph1)

    v_list: list[int] = [f"V{i + 1}" for i in range (len(graph1))]

    def print_matrix(mat: list[list[int]], cols=v_list, index=v_list):
        print(pd.DataFrame(mat, index=index, columns=cols), '\n')

    print('Матрица инцидентности')
    print_matrix(graph1.get_matrix_incident(), graph1.get_edges_list_str())

    print('Матрица степеней вершин')
    print_matrix(graph1.get_matrix_vertex_degree())

    print('Матрица достижимости')
    print_matrix(graph1.get_matrix_reachability())

    print('Матрица расстояний')
    print_matrix(graph1.get_matrix_dist())

    print('Матрица Кирхгофа')
    print_matrix(graph1.get_matrix_kirchhoff())

    matrix2: list[list[int]] = [[0, 1, 0, 0],
                                [1, 0, 1, 0],
                                [0, 1, 0, 1],
                                [0, 0, 1, 0]]
    
    graph2: Graph = Graph.load_matrix(matrix2)
    print(graph2)

    print('Задание 2')
    print('1. Добавить вершину 5, соединить её ребром с вершиной 2, затем выполнить стягивание вершин 3 и 4')
    graph2.put(5, 2, 1, non_oriented=True)
    print(graph2)

    graph2 = graph2.retraction_vertexes(3, 4)
    print(graph2)

    matrix3: list[list[int]] = [[0, 1, 0],
                                [1, 0, 1],
                                [0, 1, 0]]
    
    graph3: Graph = Graph.load_matrix(matrix3, [5, 6, 7])
    print(graph3)

    graph4: Graph = graph2.concat_graphs_by_node(graph3, 2)
    print(graph4)
