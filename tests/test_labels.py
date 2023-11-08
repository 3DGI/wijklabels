from wijklabels.labels import parse_energylabel_ditributions


def test_parse_energy_label_distributions(excelloader):
    df = parse_energylabel_ditributions(excelloader)
    print(df)
