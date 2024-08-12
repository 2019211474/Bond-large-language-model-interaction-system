import json
from py2neo import Graph, Path
from py2neo.data import Node
from py2neo.cypher import Cursor
from typing import List, Tuple
from src.utils.connect import Neo4jConnect


class Neo4jDao:
    """
    Neo4jDao用于与neo4j进行交互的类，在使用之前需要初始化Neo4jConnect，详细
    初始化请参考 :class:`Neo4jConnect`类
    """

    def __init__(self):
        """
        # 获取neo4j的操作连接，在获取之前必须保证 :class:`Neo4jConnect.__init__`被初始化
        """
        self.connect: Graph = Neo4jConnect.get_connect()

    @staticmethod
    def to_dict(ele):
        if isinstance(ele, list) or isinstance(ele, tuple):
            _ele = []
            for i in ele:
                _ele.append(Neo4jDao.to_dict(i))
            return _ele
        elif isinstance(ele, Node):
            _ele = {}
            for key in ele:
                _ele[key] = Neo4jDao.to_dict(ele[key])
            return {"Node": _ele}
        elif str(type(ele)) == "<class 'py2neo.data.HAS_DEBT'>":
            _ele = {}
            for key in ele:
                _ele[key] = Neo4jDao.to_dict(ele[key])
            return {"HAS_DEBT": _ele}
        elif str(type(ele)) == "<class 'py2neo.data.HAS_PARENT'>":
            _ele = {}
            for key in ele:
                _ele[key] = Neo4jDao.to_dict(ele[key])
            return {"HAS_PARENT": _ele}
        elif isinstance(ele, dict):
            _ele = {}
            for key in ele:
                _ele[key] = Neo4jDao.to_dict(ele[key])
            return _ele
        elif isinstance(ele, Path):
            return Neo4jDao.to_dict(ele.__dict__['_Walkable__sequence'])
        else:
            return ele

    def _select(self, cql: str, skip: int = 0, limit: int = -1, **kwargs) -> Tuple[Cursor, str]:
        """
        # 执行查询语句
        :param cql: 查询语句的主体
        :param skip: 跳过开头的条数
        :param limit: 限制页数
        :return: Cursor和查询得到的数据
        """
        cql += f" SKIP {skip}"
        if limit > 0:
            cql += f" LIMIT {limit}"
        cursor = self.connect.run(cql, **kwargs)
        _data: list = cursor.data()
        try:
            data: str = json.dumps(_data, ensure_ascii=False)
        except TypeError as e:
            print(e)
            data: str = json.dumps(self.to_dict(_data), ensure_ascii=False)
        return cursor, data

    def select(self, cql: str, skip: int = 0, limit: int = -1, **kwargs) -> Tuple[dict, str]:
        """
        # 融合:class:`Neo4jDao._select`，和:class:`Neo4jDao.select_count`
        :param cql:
        :param skip:
        :param limit:
        :param kwargs:
        :return:
        """
        count: dict = self.select_count(cql, **kwargs)
        _, data = self._select(cql, skip=skip, limit=limit, **kwargs)
        return count, data

    def select_count(self, cql: str, **kwargs) -> dict:
        """
        # 查询MATCH带有RETURN的结果数量
        :param cql: 查询语句
        :return: {'total': num}
        """
        cql = cql.split("RETURN")[0] + " RETURN COUNT(*) AS total"
        cursor = self.connect.run(cql, **kwargs)
        return cursor.data()

    def get_parent_node(self, children: List[str], *args, **kwargs) -> Tuple[dict, str]:
        """
        # 查询某些节点的父节点
        :param children: 子节点的名称列表
        :param args: 详细见 :class:`Neo4jDao.select`
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 返回查询的到的数据
        """
        cql: str = """
            MATCH (child:Level2)-[edge:HAS_PARENT]->(parent:Level1)
            WHERE child.name IN $children
            RETURN child, edge, parent
        """
        count, data = self.select(cql, *args, children=children, **kwargs)
        return count, data

    def get_child_node(self, parents: List[str], *args, **kwargs) -> Tuple[dict, str]:
        """
        # 查询某些父节点的全部子节点
        :param parents: 父节点名称列表
        :param args: 详细见 :class:`Neo4jDao.select`
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 返回查询的到的数据
        """
        cql: str = f"""
            MATCH (child:Level2)-[edge:HAS_PARENT]->(parent:Level1)
            WHERE parent.name IN $parents
            RETURN child, edge, parent
        """
        count, data = self.select(cql, *args, parents=parents, **kwargs)
        return count, data

    def get_child_receivables(self, child: str, *args, **kwargs) -> Tuple[dict, str]:
        """
        # 查询某个子节点的应收账款
        :param child: 子节点名称
        :param args: 详细见 :class:`Neo4jDao.select`
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 返回查询的应收账款
        """
        cql: str = f"""
            MATCH (other:Level2)-[edge:HAS_DEBT]->(child:Level2)
            WHERE child.name = $child
            RETURN other, edge, child
        """
        count, data = self.select(cql, *args, child=child, **kwargs)
        return count, data

    def get_child_debt(self, child: str, *args, **kwargs) -> Tuple[dict, str]:
        """
        # 查询某个子节点的欠款
        :param child: 子节点名称
        :param args: 详细见 :class:`Neo4jDao.select`
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 返回查询的欠款
        """
        cql: str = f"""
            MATCH (child:Level2)-[edge:HAS_DEBT]->(other:Level2)
            WHERE child.name = $child
            RETURN child, edge, other
        """
        count, data = self.select(cql, *args, child=child, **kwargs)
        return count, data

    def get_parent_receivables(self, parent: str, *args, **kwargs) -> Tuple[dict, str]:
        """
        # 获取某个父节点的全部应收账款
        :param parent: 父节点名称
        :param args: 详细见 :class:`Neo4jDao.select`
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 全部应收账款数据
        """
        cql: str = f"""
            MATCH (child:Level2)-[:HAS_PARENT]->(parent:Level1)
            WHERE parent.name = $parent
            WITH child
            MATCH (other:Level2)-[edge:HAS_DEBT]->(child)
            RETURN other, edge, child
        """
        count, data = self.select(cql, *args, parent=parent, **kwargs)
        return count, data

    def get_parent_debt(self, parent: str, *args, **kwargs) -> Tuple[dict, str]:
        """
        # 获取某个父节点的全部债务
        :param parent: 父节点名称
        :param args: 详细见 :class:`Neo4jDao.select`
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 全部债务数据
        """
        cql: str = f"""
            MATCH (child:Level2)-[:HAS_PARENT]->(parent:Level1)
            WHERE parent.name = $parent
            WITH child
            MATCH (child)-[edge:HAS_DEBT]->(other:Level2)
            RETURN child, edge, other
        """
        count, data = self.select(cql, *args, parent=parent, **kwargs)
        return count, data

    def get_parent_debt_by_details(self, parent: str, details: List[str], *args, **kwargs) -> Tuple[dict, str]:
        """
        # 根据details查询某个父节点的全部债务
        :param parent: 父节点名称
        :param details: 债务类别list
        :param args: 详细见 :class:`Neo4jDao.select`
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 返回该父节点的全部details债务
        """
        cql: str = """
            MATCH (child:Level2)-[:HAS_PARENT]->(parent:Level1)
            WHERE parent.name = $parent
            WITH child
            MATCH (child)-[edge:HAS_DEBT]->(other:Level2)
            WHERE edge.s_detail in $details
            RETURN child, edge, other
        """
        count, data = self.select(cql, parent=parent, details=details, *args, **kwargs)
        return count, data

    def get_parent_receivables_by_details(self, parent: str, details: List[str], *args, **kwargs) -> Tuple[dict, str]:
        """
        # 根据details查询某个父节点的全部应收账款
        :param parent: 父节点名称
        :param details: 债务类别list
        :param args: 详细见 :class:`Neo4jDao.select`
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 返回该父节点的全部details应收账款
        """
        cql: str = """
            MATCH (child:Level2)-[:HAS_PARENT]->(parent:Level1)
            WHERE parent.name = $parent
            WITH child
            MATCH (other:Level2)-[edge:HAS_DEBT]->(child)
            WHERE edge.s_detail in $details
            RETURN other, edge, child
        """
        count, data = self.select(cql, parent=parent, details=details, *args, **kwargs)
        return count, data

    def get_child_debt_by_details(self, child: str, details: List[str], *args, **kwargs) -> Tuple[dict, str]:
        """
        # 根据details查询某个子节点的全部欠款
        :param child: 子节点名称
        :param details: 债务类别list
        :param args: 详细见 :class:`Neo4jDao.select`
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 返回该子节点的全部details欠款
        """
        cql: str = """
            MATCH (child)-[edge:HAS_DEBT]->(other:Level2)
            WHERE child.name = $child AND edge.s_detail in $details
            RETURN child, edge, other
        """
        count, data = self.select(cql, child=child, details=details, *args, **kwargs)
        return count, data

    def get_child_receivables_by_details(self, child: str, details: List[str], *args, **kwargs) -> Tuple[dict, str]:
        """
        # 根据details查询某个子节点的全部应收账款
        :param child: 子节点名称
        :param details: 债务类别list
        :param args: 详细见 :class:`Neo4jDao.select`
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 返回该子节点的全部details应收账款
        """
        cql: str = """
            MATCH (other:Level2)-[edge:HAS_DEBT]->(child)
            WHERE child.name = $child AND edge.s_detail in $details
            RETURN other, edge, child
        """
        count, data = self.select(cql, child=child, details=details, *args, **kwargs)
        return count, data

    def get_child_ring(self, child: str, *args, jump: int = 2, **kwargs) -> Tuple[dict, str]:
        """
        # 获得某个子节点的存在的环，环有几条边通过jump控制
        :param child: 子节点名称
        :param args: 详细见 :class:`Neo4jDao.select`
        :param jump: 环的边的数目
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 返回环的信息
        """
        cql: str = f"""
            MATCH path=(child:Level2)-[*{jump}]->(child:Level2)
            WHERE child.name = $child AND SIZE(apoc.coll.toSet(nodes(path))) = $jump
            RETURN path
        """
        count, data = self.select(cql, child=child, jump=jump, *args, **kwargs)
        return count, data

    def get_parent_ring(self, parent: str, *args, jump: int = 2, **kwargs) -> Tuple[dict, str]:
        """
        # 查询某个父节点的全部子节点所存在的jump个边环的信息
        :param parent: 父节点名称
        :param args: 详细见 :class:`Neo4jDao.select`
        :param jump: 环的边的数目
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 返回环的信息
        """
        cql: str = f"""
            MATCH (child:Level2)-[edge:HAS_PARENT]->(parent:Level1)
            WHERE parent.name = $parent
            WITH child, parent, edge
            MATCH path=(child:Level2)-[*{jump}]->(child:Level2)
            WHERE SIZE(apoc.coll.toSet(nodes(path))) = $jump
            RETURN path, child, edge, parent
        """
        count, data = self.select(cql, parent=parent, jump=jump, *args, **kwargs)
        return count, data

    def get_child_with_child_ring(self, child1: str, child2: str, jump: int = 2, *args, **kwargs) -> Tuple[dict, str]:
        """
        # 查询child1->...child2->...child1存在的jump边的环
        :param child1: 子节点1的名称
        :param child2: 子节点2的名称
        :param args: 详细见 :class:`Neo4jDao.select`
        :param jump: 环的边的数目
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 返回环的信息
        """
        cql: str = f"""
            MATCH path=(child:Level2)-[*{jump}]->(child:Level2)
            WHERE child.name = $child1 AND SIZE(apoc.coll.toSet(nodes(path))) = $jump AND ANY(n IN nodes(path) WHERE n.name=$child2)
            RETURN path
        """
        count, data = self.select(cql, child1=child1, child2=child2, jump=jump, *args, **kwargs)
        return count, data

    def get_parent_with_parent_ring(self, parent1: str, parent2: str, jump: int = 2, *args, **kwargs) -> Tuple[dict, str]:
        """
        # 查询两个父节点之间子节点存在的jump个边的环信息
        :param parent1: 父节点1的名称
        :param parent2: 父节点2的名称
        :param args: 详细见 :class:`Neo4jDao.select`
        :param jump: 环的边的数目
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 返回环的信息
        """
        cql: str = f"""
            MATCH (child1:Level2)-[]->(parent1:Level1)
            WHERE parent1.name = $parent1
            MATCH (child2:Level2)-[]->(parent2:Level1)
            WHERE parent2.name = $parent2
            WITH child1, child2,parent1,parent2
            MATCH path=(child1:Level2)-[*{jump}]->(child1:Level2)
            WHERE SIZE(apoc.coll.toSet(nodes(path))) = $jump AND ANY(n IN nodes(path) WHERE n=child2)
            RETURN path,child1, child2,parent1,parent2
        """
        count, data = self.select(cql, parent1=parent1, parent2=parent2, jump=jump, *args, **kwargs)
        return count, data

    def get_child_to_child_debt(self, child1: str, child2: str, *args, **kwargs) -> Tuple[dict, str]:
        """
        # 查询从child1到child2的欠债
        :param child1: 子节点1名称
        :param child2: 子节点2名称
        :param args: 详细见 :class:`Neo4jDao.select`
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 返回child1到child2的欠债
        """
        cql: str = """
            MATCH (child1:Level2)-[edge:HAS_DEBT]->(child2:Level2)
            WHERE child1.name = $child1 AND child2.name = $child2
            RETURN child1, edge, child2
        """
        count, data = self.select(cql, child1=child1, child2=child2, *args, **kwargs)
        return count, data

    def get_parent_to_parent_debt(self, parent1: str, parent2: str,  *args, **kwargs) -> Tuple[dict, str]:
        """
        # 查询父节点1到父节点2的欠债
        :param parent1: 父节点1的名称
        :param parent2: 父节点2的名称
        :param args: 详细见 :class:`Neo4jDao.select`
        :param kwargs: 详细见 :class:`Neo4jDao.select`
        :return: 返回父节点1到父节点2的欠债
        """
        cql: str = """
            MATCH (child1:Level2)-[]->(parent1:Level1)
            WHERE parent1.name = $parent1
            MATCH (child2:Level2)-[]->(parent2:Level1)
            WHERE parent2.name = $parent2
            WITH child1, child2,parent1,parent2
            MATCH (child1:Level2)-[edge:HAS_DEBT]->(child2:Level2)
            RETURN parent1, child1, edge, parent2, child2
        """
        count, data = self.select(cql, parent1=parent1, parent2=parent2, *args, **kwargs)
        return count, data

    def get_child_to_child_debt_by_details(self, child1: str, child2: str, details: List[str], *args, **kwargs):
        cql: str = """
            MATCH (child1:Level2)-[edge:HAS_DEBT]->(child2:Level2)
            WHERE child1.name = $child1 AND child2.name = $child2 AND edge.s_detail in $details
            RETURN child1, edge, child2
        """
        count, data = self.select(cql, child1=child1, child2=child2, details=details, *args, **kwargs)
        return count, data


if __name__ == "__main__":
    Neo4jConnect(config_path="../utils/config.yml")
    c, res = Neo4jDao().get_child_to_child_debt_by_details("山东省公路桥梁建设集团有限公司桥梁养护科技公司", "山东高速工程检测有限公司", details=["通用"], jump=3, limit=100)
    print(c, res)
