import json

import pytest

from function import (
    CacheHandler,
    DataHandler,
    assign_molecule,
    create_molecule,
    delete_molecule,
    find_molecules_that_need_atom_updates,
    populate_cache,
    populate_cache_for_molecules,
    unassign_molecule,
)


@pytest.fixture(scope="function", autouse=True)
def setup():
    with open("tests/data.json", "r") as f:
        data = json.load(f)
    with open("tests/tmp/data.json", "w") as f:
        json.dump(data, f)
    with open("tests/tmp/cache.json", "w") as f:
        json.dump({}, f)


def test_add_molecule():
    new_name = "Temp"
    children = ["M1", "M2"]
    molecule = {"name": new_name, "children": children}

    create_molecule(molecule)

    data = DataHandler()
    M2 = data.get_item("M2")
    M1 = data.get_item("M1")
    temp = data.get_item("Temp")

    assert "Temp" in M2["parents"]
    assert "Temp" in M1["parents"]
    assert temp.get("parents") is None
    assert len(temp["children"]) == 2
    assert "M2" in temp["children"]
    assert "M1" in temp["children"]


def test_delete_molecule():
    name = "M2"

    delete_molecule(name)

    data = DataHandler()
    with pytest.raises(Exception):
        data.get_item("M2")
    # children
    A2 = data.get_item("A2")
    A3 = data.get_item("A3")
    # parents
    SuperM = data.get_item("SuperM")

    assert name not in A2["parents"]
    assert name not in A3["parents"]
    assert name not in SuperM["children"]


def test_assign_molecule():
    source_name = "M1"
    target_name = "M2"

    assign_molecule(source_name=source_name, target_name=target_name)

    data = DataHandler()
    source = data.get_item(source_name)
    target = data.get_item(target_name)
    assert len(source["parents"]) == 2
    assert target_name in source["parents"]
    assert len(target["children"]) == 3
    assert source_name in target["children"]


def test_unassign_molecule():
    source_name = "M1"
    target_name = "M4"

    unassign_molecule(source_name=source_name, target_name=target_name)

    data = DataHandler()
    source = data.get_item(source_name)
    target = data.get_item(target_name)
    assert len(source["parents"]) == 0
    assert target_name not in source["parents"]
    assert len(target["children"]) == 2
    assert source_name not in target["children"]


def test_populate_cache_from_scratch():
    populate_cache()

    cache = CacheHandler()
    what_it_should_be = {
        "M2": "A2,A3",
        "M1": "A4",
        "M4": "A1,A3,A4",
        "SuperM": "A2,A3,A1,A3,A4",
    }

    for key, value in what_it_should_be.items():
        assert set(value.split(",")) == set(cache.fetch(key=key).split(","))
        
    assert len(cache.data.keys()) == 4


def test_find_molecules_that_need_atom_updates():
    molecules = find_molecules_that_need_atom_updates("M1")

    assert len(molecules) == 3
    assert "M1" in molecules
    assert "SuperM" in molecules
    assert "M4" in molecules


def test_cache_update_after_molecule_unassigned():
    populate_cache()

    source_name = "A4"
    target_name = "M1"

    unassign_molecule(source_name=source_name, target_name=target_name)

    molecules = find_molecules_that_need_atom_updates("M1")
    populate_cache_for_molecules(molecules)

    cache = CacheHandler()
    assert cache.fetch(key="M1") is None
    assert set(cache.fetch(key="M4").split(",")) == {"A1","A3"}
    assert set(cache.fetch(key="SuperM").split(",")) == {"A1","A2","A3"}
