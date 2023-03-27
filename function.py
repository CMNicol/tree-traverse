import json
from typing import List

DATA_PATH = "tests/tmp/data.json"
CACHE_PATH = "tests/tmp/cache.json"

class DataHandler:
    def __init__(self):
        with open(DATA_PATH, "r") as f:
            self.data: List[dict] = json.load(f)

    def get_item(self, name: str):
        for item in self.data:
            if item["name"] == name:
                return item
        raise Exception(f"DB Item with name={name} not found.")

    def save(self, item: dict):
        self.data.append(item)
        with open(DATA_PATH, "w") as f:
            json.dump(self.data, f)

    def save_full_dataset(self):
        with open(DATA_PATH, "w") as f:
            json.dump(self.data, f)

    def update(self, name: str, field_to_update: str, value_to_update):
        for i, existing in enumerate(self.data):
            if existing["name"] == name:
                new = existing
                new[field_to_update] = value_to_update
                self.data.pop(i)
                break
        self.save(new)

    def delete(self, name):
        for i, existing in enumerate(self.data):
            if existing["name"] == name:
                self.data.pop(i)
                self.save_full_dataset()
                break


class CacheHandler:
    def __init__(self):
        with open(CACHE_PATH, "r") as f:
            self.data: List[dict] = json.load(f)

    def fetch(self, key: str):
        if atom_names := self.data.get(key):
            return atom_names

    def save(self, key: str, value: str):
        self.data[key] = value
        with open(CACHE_PATH, "w") as f:
            json.dump(self.data, f)
    
    def clear(self, key):
        self.data.pop(key)
        with open(CACHE_PATH, "w") as f:
            json.dump(self.data, f)

def is_an_atom(obj):
    attributes = {"properties"}
    obj_attributes = set(obj.keys())
    return attributes.issubset(obj_attributes)


def is_highest_level_molecule(obj):
    attributes = {"parents"}
    obj_attributes = set(obj.keys())
    return not attributes.issubset(obj_attributes)


def get_all_atoms_for_molecules(molecule: dict, data: DataHandler, cache: CacheHandler) -> list:
    """Recursively traverses the molecule/atom graph,
    accumulating all atoms associated with the molecule.
    """
    if atom_names := cache.fetch(molecule["name"]):
        atoms = []
        for name in atom_names.split(","):
            atoms.append(data.get_item(name))
        return atoms

    if is_an_atom(molecule):
        return [molecule]

    children = molecule["children"]
    atoms = []
    for sub_name in children:
        item = data.get_item(sub_name)
        atoms.extend(get_all_atoms_for_molecules(item, data, cache))

    cache.save(
        key=molecule["name"],
        value=",".join(
            [atom["name"] for atom in atoms],
        ),
    )

    return atoms


def get_atoms_for_molecule(name: str, data: DataHandler, cache: CacheHandler) -> list:
    item = data.get_item(name)
    return get_all_atoms_for_molecules(item, data, cache) if item else []


def populate_cache():
    # Loop through highest level nodes
    # High level nodes are objects without a `parents` attribute
    cache: CacheHandler = CacheHandler()
    data: DataHandler = DataHandler()
    for molecule in data.data:
        if is_highest_level_molecule(molecule):
            get_atoms_for_molecule(molecule["name"], data, cache)


def create_molecule(item: dict):
    data: DataHandler = DataHandler()
    if is_an_atom(item):
        data.save(item)

    for child_name in item["children"]:
        child = data.get_item(child_name)
        if is_highest_level_molecule(child):
            child["parents"] = []
        child["parents"].append(item["name"])
        data.update(
            name=child_name, field_to_update="parents", value_to_update=child["parents"]
        )

    data.save(item)


def delete_molecule(name):
    data: DataHandler = DataHandler()
    molecule_being_deleted = data.get_item(name)

    if not is_an_atom(molecule_being_deleted):
        # update `parents` of children
        for child_name in molecule_being_deleted["children"]:
            child = data.get_item(child_name)
            new_parents = [
                parent
                for parent in child["parents"]
                if parent != molecule_being_deleted["name"]
            ]
            data.update(
                name=child_name, field_to_update="parents", value_to_update=new_parents
            )

    # update `children` of parents
    if not is_highest_level_molecule(molecule_being_deleted):
        for parent_name in molecule_being_deleted["parents"]:
            parent = data.get_item(parent_name)
            new_children = [
                child
                for child in parent["children"]
                if child != molecule_being_deleted["name"]
            ]
            data.update(
                name=parent_name,
                field_to_update="children",
                value_to_update=new_children,
            )

    data.delete(name)


def assign_molecule(source_name: str, target_name: str):
    data: DataHandler = DataHandler()
    source = data.get_item(source_name)
    target = data.get_item(target_name)

    # update `children` of target
    data.update(
        name=target_name,
        field_to_update="children",
        value_to_update=target["children"] + [source_name],
    )

    # update `parents` of source
    if is_highest_level_molecule(source):
        source["parents"] = []
    data.update(
        name=source_name,
        field_to_update="parents",
        value_to_update=source["parents"] + [target_name],
    )


def unassign_molecule(source_name: str, target_name: str):
    data: DataHandler = DataHandler()
    source = data.get_item(source_name)
    target = data.get_item(target_name)

    # update `children` of target
    data.update(
        name=target_name,
        field_to_update="children",
        value_to_update=[child for child in target["children"] if child != source_name],
    )

    # update `parents` of source
    data.update(
        name=source_name,
        field_to_update="parents",
        value_to_update=[
            parent for parent in source["parents"] if parent != target_name
        ],
    )

def find_molecules_that_need_atom_updates(molecule_that_got_changed_name: str):
    data: DataHandler = DataHandler()
    molecule_that_got_changed = data.get_item(molecule_that_got_changed_name)
    if is_highest_level_molecule(molecule_that_got_changed):
        return [molecule_that_got_changed["name"]]

    parents = molecule_that_got_changed["parents"]
    atoms = []
    for sub_name in parents:
        item = data.get_item(sub_name)
        atoms.extend(find_molecules_that_need_atom_updates(item["name"]))
    atoms.extend([molecule_that_got_changed["name"]])
    return atoms

def populate_cache_for_molecules(molecule_names: List[str]):
    cache: CacheHandler = CacheHandler()
    data: DataHandler = DataHandler()
    for molecule_name in molecule_names:
        cache.clear(molecule_name)
    for molecule_name in molecule_names:
        get_atoms_for_molecule(molecule_name, data, cache)
