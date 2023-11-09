from wijklabels.labels import parse_energylabel_ditributions, reshape_for_classification


def test_parse_energy_label_distributions(excelloader):
    df = parse_energylabel_ditributions(excelloader)
    print(df)


def test_reshape_for_classification(excelloader):
    label_distributions = parse_energylabel_ditributions(excelloader)
    res = reshape_for_classification(label_distributions)
    print(res)
