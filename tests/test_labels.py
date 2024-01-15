from pytest import mark
from wijklabels.labels import parse_energylabel_ditributions, \
    reshape_for_classification, EnergyLabel


def test_parse_energy_label_distributions(excelloader):
    df = parse_energylabel_ditributions(excelloader)
    print(df)


def test_reshape_for_classification(excelloader):
    label_distributions = parse_energylabel_ditributions(excelloader)
    res = reshape_for_classification(label_distributions)
    print(res)


@mark.parametrize(
    ("_self", "other", "range", "result"),
    (
            (EnergyLabel.B, EnergyLabel.B, 0, True),
            (EnergyLabel.B, EnergyLabel.A, 0, False),
            (EnergyLabel.B, EnergyLabel.A, 1, True),
            (EnergyLabel.B, EnergyLabel.C, 1, True),
            (EnergyLabel.B, EnergyLabel.B, 1, True),
            (EnergyLabel.B, EnergyLabel.D, 1, False),
            (EnergyLabel.B, EnergyLabel.APPP, 1, False),
            (EnergyLabel.G, EnergyLabel.E, 2, True),
            (EnergyLabel.G, EnergyLabel.APPPP, 2, False),
            (EnergyLabel.APPPP, EnergyLabel.G, 2, False),
    )
)
def test_within(_self, other, range, result):
    assert _self.within(other, range) == result


@mark.parametrize(
    ("_self", "other", "result"),
    (
            (EnergyLabel.B, EnergyLabel.B, 0),
            (EnergyLabel.A, EnergyLabel.D, -3),
            (EnergyLabel.D, EnergyLabel.A, 3),
            (EnergyLabel.G, EnergyLabel.APPPP, 10),
    )
)
def test_distance(_self, other, result):
    assert _self.distance(other) == result