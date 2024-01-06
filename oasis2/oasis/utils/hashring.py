import hashlib


class HashRing:
    def __init__(self, nodes, n_num=3):
        self.hash_dict = {}
        self.n_num = n_num
        for node in nodes:
            self.add_node(node)

    def add_node(self, *nodes):
        for node in nodes:
            for i in range(self.n_num):
                key = hashlib.md5(f'{node}-{i}'.encode('utf8')).hexdigest()
                self.hash_dict.setdefault(key, node)
        self.hash_dict = {k: self.hash_dict.get(k) for k in sorted(self.hash_dict.keys())}

    def remove_node(self, node):
        rm_keys = [k for k, v in self.hash_dict.items() if v == node]
        for rm_key in rm_keys:
            self.hash_dict.pop(rm_key)

    def get_node(self, job_id):
        get_key = hashlib.md5(job_id.encode('utf8')).hexdigest()
        default_k = None
        for k in self.hash_dict.keys():
            if get_key <= k:
                return self.hash_dict.get(k)
            default_k = k
        return self.hash_dict.get(default_k)
