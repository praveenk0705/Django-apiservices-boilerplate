"""Data Access Objects Class Implementation

This class creates some useful CRUD operation objects.
Available operations:
1. create    - insert new record(s)
2. update    - update record(s) in the table
3. findAll   - find all records in the table
4. findFirst - find the first record in the table
5. findLast  - find the last record in the table
"""
from django.db import connection
from collections import namedtuple

class dao:
    def __init__(self, table, key):
        self.cursor = connection.cursor()
        self.table = table
        self.key = key

    def __del__(self):
        self.cursor.close()

    # Private method to return all rows with column names
    def __cleanFetchAll(self, cursor):
        desc = cursor.description
        nt_result = namedtuple('Result', [col[0] for col in desc])

        return [nt_result(*row) for row in cursor.fetchall()]

    # Private method to return one row with column names
    def __cleanFetchOne(self, cursor):
        desc = cursor.description
        nt_result = namedtuple('Result', [col[0] for col in desc])
        result = cursor.fetchone()

        return () if result is None else nt_result(*result)

    # @param    attributes      array of dictionaries that contain all new records
    # @return   Boolean
    def create(self, attributes):
        retArr = []
        for attribute in attributes:
            columns = ", ".join(map(str, list(attribute.keys())))
            values = ", ".join(map("'{0}'".format, list(map(str, list(attribute.values())))))

            query = "INSERT INTO " + self.table + " (" + columns + ") VALUES (" + values + ")"

            print("create query:", query)
            self.cursor.execute(query)
            retArr.append(self.cursor.lastrowid)

        return retArr

    # @param    attributes      array of dictionaries that contain all new records
    # @return   Boolean
    def replace(self, attributes):
        retArr = []
        for attribute in attributes:
            columns = ", ".join(map(str, list(attribute.keys())))
            values = ", ".join(map("'{0}'".format, list(map(str, list(attribute.values())))))

            query = "REPLACE INTO " + self.table + " (" + columns + ") VALUES (" + values + ")"

            print("replace query:", query)
            self.cursor.execute(query)
            retArr.append(self.cursor.lastrowid)

        return retArr

    # @param    attribute       dictionaries of all columns to be updated
    # @param    requirements    dictionaries of all update requirements
    # @return   Boolean
    def update(self, attribute, requirements):
        columns = ", ".join("{0} = '{1}'".format(key, value) for (key, value) in list(attribute.items()))
        reqs = " AND ".join("{0} = '{1}'".format(key, value) for (key, value) in list(requirements.items()))

        query = "UPDATE " + self.table + " SET " + columns + " WHERE " + reqs
        #print("update query:", query)
        self.cursor.execute(query)

        return self.cursor.lastrowid

    # @param    requirements    dictionaries of all select requirements
    # @return   Array
    def findAll(self, requirements, sort_by = None, limit = None):
        reqs = " AND ".join("{0} = '{1}'".format(key, value) for (key, value) in list(requirements.items()))

        sorting = ""
        if sort_by:
            sorting = " order by " + str(sort_by)

        limit_str = ""
        if limit:
            limit_str = " limit " + str(limit)

        query = "SELECT * FROM " + self.table + " WHERE " + reqs + sorting + limit_str
        #print("findAll query:", query)
        self.cursor.execute(query)


        return self.__cleanFetchAll(self.cursor)

    # @param    requirements    dictionaries of all select requirements
    # @return   Array
    def findInAll(self, requirements, in_reqs = None, sort_by = None, limit = None):
        reqs = " AND ".join("{0} = '{1}'".format(key, value) for (key, value) in list(requirements.items()))

        in_reqs_str = ""
        for (i_key, i_value) in list(in_reqs.items()):
            in_reqs_str += " AND " + str(i_key) + " IN ("

            for i_val in i_value:
                in_reqs_str += "'"+i_val+"',"
            in_reqs_str = in_reqs_str[:-1]
            in_reqs_str += ") "

        sorting = ""
        if sort_by:
            sorting = " order by " + str(sort_by)

        limit_str = ""
        if limit:
            limit_str = " limit " + str(limit)

        query = "SELECT * FROM " + self.table + " WHERE " + reqs + in_reqs_str + sorting + limit_str
        self.cursor.execute(query)

        print("findInAll query: ", query)

        return self.__cleanFetchAll(self.cursor)

    # @param    query
    # @return   Array
    def runQuery(self, query, queryType = None):

        self.cursor.execute(query)
        print("runQuery query: ", query)
        if queryType == 'UPDATE':
            return True

        return self.__cleanFetchAll(self.cursor)

    # @param    query
    # @return   Array
    def runDeleteQuery(self, query):

        print("runDeleteQuery query: ", query)
        return self.cursor.execute(query)


    # @param    requirements    dictionaries of all select requirements
    # @return   Array
    def findGroupAll(self, requirements, group_by = None, sort_by = None, limit = None):
        reqs = " AND ".join("{0} = '{1}'".format(key, value) for (key, value) in list(requirements.items()))

        group_str = ""
        if group_by:
            group_str = " group by " + str(group_by)

        sorting = ""
        if sort_by:
            sorting = " order by " + str(sort_by)

        limit_str = ""
        if limit:
            limit_str = " limit " + str(limit)

        query = "SELECT " + str(group_by) + " FROM " + self.table + " WHERE " + reqs + group_str + sorting + limit_str
        self.cursor.execute(query)

        print("findGroupAll query: ", query)

        return self.__cleanFetchAll(self.cursor)


    # @param    requirements    dictionaries of all select requirements
    # @return   Tuple
    def findFirst(self, requirements):
        reqs = " AND ".join("{0} = '{1}'".format(key, value) for (key, value) in list(requirements.items()))

        query = "SELECT * FROM " + self.table + " WHERE " + reqs + " LIMIT 1"
        #print("findFirst query:", query)
        self.cursor.execute(query)

        return self.__cleanFetchOne(self.cursor)

    # @param    requirements    dictionaries of all select requirements
    # @return   Tuple
    def findLast(self, requirements, order_key = None):
        if order_key is None:
            order_key = self.key

        reqs = " AND ".join("{0} = '{1}'".format(key, value) for (key, value) in list(requirements.items()))

        query = "SELECT * FROM " + self.table + " WHERE " + reqs + " ORDER BY " + order_key + " DESC LIMIT 1"
        self.cursor.execute(query)

        return self.__cleanFetchOne(self.cursor)
