
from tasks import somar, fatorial


def test_somar():
    assert somar.run(2, 3) == 5


def test_fatorial():
    assert fatorial.run(5) == 120


def test_fatorial_zero():
    assert fatorial.run(0) == 1


def test_fatorial_numero_negativo():
    try:
        fatorial.run(-1)
        assert False, "Era esperado um ValueError"
    except ValueError:
        pass
