import numpy as np

# The FactSet industry to GICS sector mapping used to emulate GICS1 with FactSet data.

gic_code_to_gic_name = {
    10: "Energy",
    15: "Materials",
    20: "Industrials",
    25: "Consumer_Discretionary",
    30: "Consumer_Staples",
    35: "Health_Care",
    40: "Financials",
    45: "Information_Technology",
    50: "Communication_Services",
    55: "Utilities",
    60: "Real_Estate",
    90: "Not_Classified"
}

spdr_to_gic_code =  {"XLE": 10,
                    "XLB": 15,
                    "XLI": 20,
                    "XLY": 25,
                    "XLP": 30,
                    "XLV": 35,
                    "XLF": 40,
                    "XLK": 45,
                    "XLC": 50,
                    "XLU": 55,
                    "XLRE": 60,
                    }


fs_industry_to_gic_code = {
    # fs_industry_code,fs_industry,fs_sector_code,fs_sector,gics_sector_code,gics_sector
    1105: 15,  # 1105,Steel,1100,Non-Energy Minerals,15,Materials
    1115: 15,  # 1115,Aluminum,1100,Non-Energy Minerals,15,Materials
    1120: 15,  # 1120,Precious Metals,1100,Non-Energy Minerals,15,Materials
    1125: 15,  # 1125,Other Metals/Minerals,1100,Non-Energy Minerals,15,Materials
    1130: 15,  # 1130,Forest Products,1100,Non-Energy Minerals,15,Materials
    1135: 15,  # 1135,Construction Materials,1100,Non-Energy Minerals,15,Materials
    1205: 15,  # 1205,Metal Fabrication,1200,Producer Manufacturing,15,Materials
    1210: 20,  # 1210,Industrial Machinery,1200,Producer Manufacturing,20,Industrials
    1220: 20,  # 1220,Trucks/Construction/Farm Machinery,1200,Producer Manufacturing,20,Industrials
    1225: 25,  # 1225,Auto Parts: OEM,1200,Producer Manufacturing,25,Consumer Discretionary
    1230: 20,  # 1230,Building Products,1200,Producer Manufacturing,20,Industrials
    1235: 20,  # 1235,Electrical Products,1200,Producer Manufacturing,20,Industrials
    1245: 20,  # 1245,Office Equipment/Supplies,1200,Producer Manufacturing,20,Industrials
    1250: 20,  # 1250,Miscellaneous Manufacturing,1200,Producer Manufacturing,20,Industrials
    1255: 20,  # 1255,Industrial Conglomerates,1200,Producer Manufacturing,20,Industrials
    1305: 45,  # 1305,Semiconductors,1300,Electronic Technology,45,Information Technology
    1310: 45,  # 1310,Electronic Components,1300,Electronic Technology,45,Information Technology
    1315: 45,  # 1315,Electronic Equipment/Instruments,1300,Electronic Technology,45,Information Technology
    1320: 45,  # 1320,Telecommunications Equipment,1300,Electronic Technology,45,Information Technology
    1330: 20,  # 1330,Aerospace & Defense,1300,Electronic Technology,20,Industrials
    1340: 45,  # 1340,Computer Processing Hardware,1300,Electronic Technology,45,Information Technology
    1345: 45,  # 1345,Computer Peripherals,1300,Electronic Technology,45,Information Technology
    1352: 45,  # 1352,Computer Communications,1300,Electronic Technology,45,Information Technology
    1355: 45,  # 1355,Electronic Production Equipment,1300,Electronic Technology,45,Information Technology
    1405: 25,  # 1405,Motor Vehicles,1400,Consumer Durables,25,Consumer Discretionary
    1410: 25,  # 1410,Automotive Aftermarket,1400,Consumer Durables,25,Consumer Discretionary
    1415: 25,  # 1415,Homebuilding,1400,Consumer Durables,25,Consumer Discretionary
    1420: 25,  # 1420,Home Furnishings,1400,Consumer Durables,25,Consumer Discretionary
    1425: 25,  # 1425,Electronics/Appliances,1400,Consumer Durables,25,Consumer Discretionary
    1430: 20,  # 1430,Tools & Hardware,1400,Consumer Durables,20,Industrials
    1435: 25,  # 1435,Recreational Products,1400,Consumer Durables,25,Consumer Discretionary
    1445: 25,  # 1445,Other Consumer Specialties,1400,Consumer Durables,25,Consumer Discretionary
    2105: 10,  # 2105,Oil & Gas Production,2100,Energy Minerals,10,Energy
    2110: 10,  # 2110,Integrated Oil,2100,Energy Minerals,10,Energy
    2120: 10,  # 2120,Oil Refining/Marketing,2100,Energy Minerals,10,Energy
    2125: 10,  # 2125,Coal,2100,Energy Minerals,10,Energy
    2205: 15,  # 2205,Chemicals: Major Diversified,2200,Process Industries,15,Materials
    2210: 15,  # 2210,Chemicals: Specialty,2200,Process Industries,15,Materials
    2215: 15,  # 2215,Chemicals: Agricultural,2200,Process Industries,15,Materials
    2220: 25,  # 2220,Textiles,2200,Process Industries,25,Consumer Discretionary
    2225: 30,  # 2225,Agricultural Commodities/Milling,2200,Process Industries,30,Consumer Staples
    2230: 15,  # 2230,Pulp & Paper,2200,Process Industries,15,Materials
    2235: 15,  # 2235,Containers/Packaging,2200,Process Industries,15,Materials
    2240: 15,  # 2240,Industrial Specialties,2200,Process Industries,15,Materials
    2305: 35,  # 2305,Pharmaceuticals: Major,2300,Health Technology,35,Health Care
    2310: 35,  # 2310,Pharmaceuticals: Other,2300,Health Technology,35,Health Care
    2315: 35,  # 2315,Pharmaceuticals: Generic,2300,Health Technology,35,Health Care
    2320: 35,  # 2320,Biotechnology,2300,Health Technology,35,Health Care
    2325: 35,  # 2325,Medical Specialties,2300,Health Technology,35,Health Care
    2405: 30,  # 2405,Food: Major Diversified,2400,Consumer Non-Durables,30,Consumer Staples
    2410: 30,  # 2410,Food: Specialty/Candy,2400,Consumer Non-Durables,30,Consumer Staples
    2415: 30,  # 2415,Food: Meat/Fish/Dairy,2400,Consumer Non-Durables,30,Consumer Staples
    2420: 30,  # 2420,Beverages: Non-Alcoholic,2400,Consumer Non-Durables,30,Consumer Staples
    2425: 30,  # 2425,Beverages: Alcoholic,2400,Consumer Non-Durables,30,Consumer Staples
    2430: 30,  # 2430,Tobacco,2400,Consumer Non-Durables,30,Consumer Staples
    2435: 30,  # 2435,Household/Personal Care,2400,Consumer Non-Durables,30,Consumer Staples
    2440: 25,  # 2440,Apparel/Footwear,2400,Consumer Non-Durables,25,Consumer Discretionary
    2450: 25,  # 2450,Consumer Sundries,2400,Consumer Non-Durables,25,Consumer Discretionary
    3105: 10,  # 3105,Contract Drilling,3100,Industrial Services,10,Energy
    3110: 10,  # 3110,Oilfield Services/Equipment,3100,Industrial Services,10,Energy
    3115: 20,  # 3115,Engineering & Construction,3100,Industrial Services,20,Industrials
    3120: 20,  # 3120,Environmental Services,3100,Industrial Services,20,Industrials
    3130: 10,  # 3130,Oil & Gas Pipelines,3100,Industrial Services,10,Energy
    3205: 20,  # 3205,Miscellaneous Commercial Services,3200,Commercial Services,20,Industrials
    3210: 25,  # 3210,Advertising/Marketing Services,3200,Commercial Services,25,Consumer Discretionary
    3215: 20,  # 3215,Commercial Printing/Forms,3200,Commercial Services,20,Industrials
    3220: 40,  # 3220,Financial Publishing/Services,3200,Commercial Services,40,Financials
    3235: 20,  # 3235,Personnel Services,3200,Commercial Services,20,Industrials
    3255: 25,  # 3255,Wholesale Distributors,3250,Distribution Services,25,Consumer Discretionary
    3260: 30,  # 3260,Food Distributors,3250,Distribution Services,30,Consumer Staples
    3265: 45,  # 3265,Electronics Distributors,3250,Distribution Services,45,Information Technology
    3270: 35,  # 3270,Medical Distributors,3250,Distribution Services,35,Health Care
    3305: 45,  # 3305,Data Processing Services,3300,Technology Services,45,Information Technology
    3308: 45,  # 3308,Information Technology Services,3300,Technology Services,45,Information Technology
    3310: 45,  # 3310,Packaged Software,3300,Technology Services,45,Information Technology
    3320: 45,  # 3320,Internet Software/Services,3300,Technology Services,45,Information Technology
    3355: 35,  # 3355,Managed Health Care,3350,Health Services,35,Health Care
    3360: 35,  # 3360,Hospital/Nursing Management,3350,Health Services,35,Health Care
    3365: 35,  # 3365,Medical/Nursing Services,3350,Health Services,35,Health Care
    3370: 35,  # 3370,Services to the Health Industry,3350,Health Services,35,Health Care
    3405: 25,  # 3405,Media Conglomerates,3400,Consumer Services,25,Consumer Discretionary
    3410: 25,  # 3410,Broadcasting,3400,Consumer Services,25,Consumer Discretionary
    3415: 25,  # 3415,Cable/Satellite TV,3400,Consumer Services,25,Consumer Discretionary
    3420: 25,  # 3420,Publishing: Newspapers,3400,Consumer Services,25,Consumer Discretionary
    3425: 25,  # 3425,Publishing: Books/Magazines,3400,Consumer Services,25,Consumer Discretionary
    3430: 25,  # 3430,Movies/Entertainment,3400,Consumer Services,25,Consumer Discretionary
    3435: 25,  # 3435,Restaurants,3400,Consumer Services,25,Consumer Discretionary
    3440: 25,  # 3440,Hotels/Resorts/Cruiselines,3400,Consumer Services,25,Consumer Discretionary
    3445: 25,  # 3445,Casinos/Gaming,3400,Consumer Services,25,Consumer Discretionary
    3450: 25,  # 3450,Other Consumer Services,3400,Consumer Services,25,Consumer Discretionary
    3505: 30,  # 3505,Food Retail,3500,Retail Trade,30,Consumer Staples
    3510: 30,  # 3510,Drugstore Chains,3500,Retail Trade,30,Consumer Staples
    3515: 25,  # 3515,Department Stores,3500,Retail Trade,25,Consumer Discretionary
    3520: 25,  # 3520,Discount Stores,3500,Retail Trade,25,Consumer Discretionary
    3525: 25,  # 3525,Apparel/Footwear Retail,3500,Retail Trade,25,Consumer Discretionary
    3530: 25,  # 3530,Home Improvement Chains,3500,Retail Trade,25,Consumer Discretionary
    3535: 25,  # 3535,Electronics/Appliance Stores,3500,Retail Trade,25,Consumer Discretionary
    3540: 25,  # 3540,Specialty Stores,3500,Retail Trade,25,Consumer Discretionary
    3545: 25,  # 3545,Catalog/Specialty Distribution,3500,Retail Trade,25,Consumer Discretionary
    3550: 25,  # 3550,Internet Retail,3500,Retail Trade,25,Consumer Discretionary
    4605: 20,  # 4605,Air Freight/Couriers,4600,Transportation,20,Industrials
    4610: 20,  # 4610,Airlines,4600,Transportation,20,Industrials
    4615: 20,  # 4615,Trucking,4600,Transportation,20,Industrials
    4620: 20,  # 4620,Railroads,4600,Transportation,20,Industrials
    4625: 20,  # 4625,Marine Shipping,4600,Transportation,20,Industrials
    4630: 20,  # 4630,Other Transportation,4600,Transportation,20,Industrials
    4705: 55,  # 4705,Electric Utilities,4700,Utilities,55,Utilities
    4735: 55,  # 4735,Gas Distributors,4700,Utilities,55,Utilities
    4755: 55,  # 4755,Water Utilities,4700,Utilities,55,Utilities
    4760: 55,  # 4760,Alternative Power Generation,4700,Utilities,55,Utilities
    4805: 40,  # 4805,Major Banks,4800,Finance,40,Financials
    4810: 40,  # 4810,Regional Banks,4800,Finance,40,Financials
    4825: 40,  # 4825,Savings Banks,4800,Finance,40,Financials
    4830: 40,  # 4830,Finance/Rental/Leasing,4800,Finance,40,Financials
    4840: 40,  # 4840,Investment Banks/Brokers,4800,Finance,40,Financials
    4845: 40,  # 4845,Investment Managers,4800,Finance,40,Financials
    4850: 40,  # 4850,Financial Conglomerates,4800,Finance,40,Financials
    4855: 40,  # 4855,Property/Casualty Insurance,4800,Finance,40,Financials
    4860: 40,  # 4860,Multi-Line Insurance,4800,Finance,40,Financials
    4865: 40,  # 4865,Life/Health Insurance,4800,Finance,40,Financials
    4875: 40,  # 4875,Specialty Insurance,4800,Finance,40,Financials
    4880: 40,  # 4880,Insurance Brokers/Services,4800,Finance,40,Financials
    4885: 60,  # 4885,Real Estate Development,4800,Finance,60,Real Estate
    4890: 60,  # 4890,Real Estate Investment Trusts,4800,Finance,60,Real Estate
    4905: 50,  # 4905,Major Telecommunications,4900,Communications,50,Telecommunication Services
    4910: 50,  # 4910,Specialty Telecommunications,4900,Communications,50,Telecommunication Services
    4915: 50,  # 4915,Wireless Telecommunications,4900,Communications,50,Telecommunication Services
    6005: 90,  # 6005,Miscellaneous,6000,Miscellaneous,90,Miscellaneous
    6010: 90,  # 6010,Investment Trusts/Mutual Funds,6000,Miscellaneous,90,Miscellaneous
    7005: np.nan,  # 7005,Sovereign,7000,Government,,
    7010: np.nan,  # 7010,Province/State,7000,Government,,
    7015: np.nan,  # 7015,Municipality,7000,Government,,
    7020: np.nan,  # 7020,National Agency,7000,Government,,
    7025: np.nan,  # 7025,State Agency,7000,Government,,
    7028: np.nan,  # 7028,Local Agencies,7000,Government,,
    7030: np.nan,  # 7030,Supranational,7000,Government,,
    7035: np.nan,  # 7035,General Government,7000,Government,,
    9999: np.nan  # 9999,Not Classified,9999,Not Classified,,
}

fs_industry_to_gic_name = {
    fs_code: gic_code_to_gic_name.get(gic_code) 
    for fs_code, gic_code in fs_industry_to_gic_code.items() 
    if gic_code_to_gic_name.get(gic_code) is not None
}

gic_name_to_spdr_ticker = {
    gic_code_to_gic_name[gic_code]: ticker
    for ticker, gic_code in spdr_to_gic_code.items()
}