# Energy labels of Dutch neighborhoods

The [Voorbeeldwoningen 2022](https://www.rvo.nl/onderwerpen/wetten-en-regels-gebouwen/voorbeeldwoningen-bestaande-bouw) study describes the distribution of energy labels per vormfactor range for each dwelling type of the [WoON 2018](https://www.woononderzoek.nl/) study.
The latest release of the [3DBAG](https://3dbag.nl) data set (`2023.10.08`) provides the surface areas for calculating the vormfactor for each Pand.
This work explores the possibility of calculating the vormfactor for each dwelling within a Pand and applying the energy label distributions of the Voorbeeldwoningen 2022 study to estimate the energy label distribution of each neighborhood of the Netherlands.

This repository contains the code for estimating the energy labels and report on the results.

The report can be viewed at: https://3dgi.github.io/wijklabels/report.html

The estimated labels can be downloaded from: https://data.3dgi.xyz/wijklabels. The files with `.fgb` are in the [FlatGeobuf](https://flatgeobuf.org/) format, which can be viewed without download in several GIS applications (e.g. QGIS). For example, to view the neighborhood labels in QGIS, open the file URL (`https://data.3dgi.xyz/wijklabels/labels_neighborhood_geom.fgb`) as a vector data source.

**License** of report and data: https://creativecommons.org/licenses/by/4.0/deed.nl. You are free to share and modify the report and the data, as long as you give appropriate credit to 3DGI, provide a link to the license, and indicate if changes were made . 

Attributes of the individual labels:

| Attribute              | Beschrijving                                                                                                             |
|------------------------|--------------------------------------------------------------------------------------------------------------------------|
| pand_identificatie     | BAG Pand identificatie                                                                                                   |
| vbo_identificatie      | BAG Verblijfsobject identificatie                                                                                        |
| oorspronkelijkbouwjaar | BAG Pand bouwjaar                                                                                                        |
| oppervlakte            | BAG Verblijfsobject oppervlakte                                                                                          |
| woningtype             | Geschat woningtype, gem. NTA8800                                                                                         |
| landcode               | Landcode, altijd “NL”                                                                                                    |
| gemeentecode           | CBS gemeentecode                                                                                                         |
| wijkcode               | CBS wijkcode                                                                                                             |
| buurtcode              | CBS buurtcode                                                                                                            |
| nr_floors              | Geschat aantal verdiepingen                                                                                              |
| vbo_count              | Aantal verblijfsobjecten in het Pand                                                                                     |
| b3_opp_buitenmuur      | 3DBAG totale oppervlakte van de buitenmuren, https://docs.3dbag.nl/nl/schema/attributes/#b3_opp_buitenmuur               |
| b3_opp_dak_plat        | 3DBAG totale oppervlakte van de platte delen van het dak, https://docs.3dbag.nl/nl/schema/attributes/#b3_opp_dak_plat    |
| b3_opp_dak_schuin      | 3DBAG totale oppervlakte van de schuine delen van het dak, https://docs.3dbag.nl/nl/schema/attributes/#b3_opp_dak_schuin |
| b3_opp_grond           | 3DBAG totale oppervlakte begane grond, https://docs.3dbag.nl/nl/schema/attributes/#b3_opp_grond                          |
| b3_opp_scheidingsmuur  | 3DBAG totale oppervlakte van de pandscheidende muren, https://docs.3dbag.nl/nl/schema/attributes/#b3_opp_scheidingsmuur  |
| woningtype_pre_nta8800 | Geschat woningtype, pre-NTA8800 methode                                                                                  |
| vormfactor             | Geschat vormfactor                                                                                                       |
| vormfactorclass        | Geschat vormfactor, gegroepeerd volgens Voorbeeldwoningen 2022                                                           |
| bouwperiode            | BAG Pand bouwjaar, gegroepeerd volgens Voorbeeldwoningen 2022                                                            |
| energylabel            | Geschat energielabel                                                                                                     |

# Funding

This work was funded by the Dutch [Rijksdienst voor Ondernemend Nederland](https://www.rvo.nl/)