import pytest
from cascade.engine import Engine, CycleError

def test_fixed_point_basic():
    engine = Engine()

    @engine.input
    def input_val():
        return 1

    @engine.query(fixed_point=0)
    def cell_a():
        return cell_b() + input_val()
        
    @engine.query
    def cell_b():
        return cell_a() // 2
        
    assert cell_a() == 1
    assert cell_b() == 0
    
    input_val.set(value=4)
    assert cell_a() == 7
    assert cell_b() == 3

def test_fixed_point_no_default_raises():
    engine = Engine()
    
    @engine.query
    def cell_a():
        return cell_b()
        
    @engine.query
    def cell_b():
        return cell_a()
        
    with pytest.raises(CycleError):
        cell_a()
