import os
import json

class Helper:
    def resolve_ids_for_query(self,ids):
        if len(ids) == 1:
            return f"({ids[0]})"
        else :
            return tuple(ids)  

    def resolve_names_for_query(self,ids):
        if len(ids) == 1:
            return f"('{ids[0]}')"
        else :
            return tuple(ids)       

    def pluck(self,lst, key):
        return [x.get(key) for x in lst]        