#!/usr/bin/env python3
"""
Extract countries by continent from Wikipedia and update continents.json

Data sourced from: https://simple.wikipedia.org/wiki/List_of_countries_by_continents
"""
import json


def get_countries_by_continent():
    """
    Return curated list of countries by continent with ISO codes based on Wikipedia data.
    Sources:
    - https://simple.wikipedia.org/wiki/List_of_countries_by_continents
    - https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
    """
    return {
        'Africa': [
            {'name': 'Algeria', 'iso': 'DZ'},
            {'name': 'Angola', 'iso': 'AO'},
            {'name': 'Benin', 'iso': 'BJ'},
            {'name': 'Botswana', 'iso': 'BW'},
            {'name': 'Burkina Faso', 'iso': 'BF'},
            {'name': 'Burundi', 'iso': 'BI'},
            {'name': 'Cameroon', 'iso': 'CM'},
            {'name': 'Cape Verde', 'iso': 'CV'},
            {'name': 'Central African Republic', 'iso': 'CF'},
            {'name': 'Chad', 'iso': 'TD'},
            {'name': 'Comoros', 'iso': 'KM'},
            {'name': 'Democratic Republic of the Congo', 'iso': 'CD'},
            {'name': 'Republic of the Congo', 'iso': 'CG'},
            {'name': 'Djibouti', 'iso': 'DJ'},
            {'name': 'Egypt', 'iso': 'EG'},
            {'name': 'Equatorial Guinea', 'iso': 'GQ'},
            {'name': 'Eritrea', 'iso': 'ER'},
            {'name': 'Eswatini', 'iso': 'SZ'},
            {'name': 'Ethiopia', 'iso': 'ET'},
            {'name': 'Gabon', 'iso': 'GA'},
            {'name': 'Gambia', 'iso': 'GM'},
            {'name': 'Ghana', 'iso': 'GH'},
            {'name': 'Guinea', 'iso': 'GN'},
            {'name': 'Guinea-Bissau', 'iso': 'GW'},
            {'name': 'Ivory Coast', 'iso': 'CI'},
            {'name': 'Kenya', 'iso': 'KE'},
            {'name': 'Lesotho', 'iso': 'LS'},
            {'name': 'Liberia', 'iso': 'LR'},
            {'name': 'Libya', 'iso': 'LY'},
            {'name': 'Madagascar', 'iso': 'MG'},
            {'name': 'Malawi', 'iso': 'MW'},
            {'name': 'Mali', 'iso': 'ML'},
            {'name': 'Mauritania', 'iso': 'MR'},
            {'name': 'Mauritius', 'iso': 'MU'},
            {'name': 'Morocco', 'iso': 'MA'},
            {'name': 'Mozambique', 'iso': 'MZ'},
            {'name': 'Namibia', 'iso': 'NA'},
            {'name': 'Niger', 'iso': 'NE'},
            {'name': 'Nigeria', 'iso': 'NG'},
            {'name': 'Rwanda', 'iso': 'RW'},
            {'name': 'São Tomé and Príncipe', 'iso': 'ST'},
            {'name': 'Senegal', 'iso': 'SN'},
            {'name': 'Seychelles', 'iso': 'SC'},
            {'name': 'Sierra Leone', 'iso': 'SL'},
            {'name': 'Somalia', 'iso': 'SO'},
            {'name': 'South Africa', 'iso': 'ZA'},
            {'name': 'South Sudan', 'iso': 'SS'},
            {'name': 'Sudan', 'iso': 'SD'},
            {'name': 'Tanzania', 'iso': 'TZ'},
            {'name': 'Togo', 'iso': 'TG'},
            {'name': 'Tunisia', 'iso': 'TN'},
            {'name': 'Uganda', 'iso': 'UG'},
            {'name': 'Zambia', 'iso': 'ZM'},
            {'name': 'Zimbabwe', 'iso': 'ZW'}
        ],
        'Antarctica': [],
        'Asia': [
            {'name': 'Afghanistan', 'iso': 'AF'},
            {'name': 'Armenia', 'iso': 'AM'},
            {'name': 'Azerbaijan', 'iso': 'AZ'},
            {'name': 'Bahrain', 'iso': 'BH'},
            {'name': 'Bangladesh', 'iso': 'BD'},
            {'name': 'Bhutan', 'iso': 'BT'},
            {'name': 'Brunei', 'iso': 'BN'},
            {'name': 'Cambodia', 'iso': 'KH'},
            {'name': 'China', 'iso': 'CN'},
            {'name': 'India', 'iso': 'IN'},
            {'name': 'Indonesia', 'iso': 'ID'},
            {'name': 'Iran', 'iso': 'IR'},
            {'name': 'Iraq', 'iso': 'IQ'},
            {'name': 'Israel', 'iso': 'IL'},
            {'name': 'Japan', 'iso': 'JP'},
            {'name': 'Jordan', 'iso': 'JO'},
            {'name': 'Kazakhstan', 'iso': 'KZ'},
            {'name': 'North Korea', 'iso': 'KP'},
            {'name': 'South Korea', 'iso': 'KR'},
            {'name': 'Kuwait', 'iso': 'KW'},
            {'name': 'Kyrgyzstan', 'iso': 'KG'},
            {'name': 'Laos', 'iso': 'LA'},
            {'name': 'Lebanon', 'iso': 'LB'},
            {'name': 'Malaysia', 'iso': 'MY'},
            {'name': 'Maldives', 'iso': 'MV'},
            {'name': 'Mongolia', 'iso': 'MN'},
            {'name': 'Myanmar', 'iso': 'MM'},
            {'name': 'Nepal', 'iso': 'NP'},
            {'name': 'Oman', 'iso': 'OM'},
            {'name': 'Pakistan', 'iso': 'PK'},
            {'name': 'Philippines', 'iso': 'PH'},
            {'name': 'Qatar', 'iso': 'QA'},
            {'name': 'Saudi Arabia', 'iso': 'SA'},
            {'name': 'Singapore', 'iso': 'SG'},
            {'name': 'Sri Lanka', 'iso': 'LK'},
            {'name': 'Syria', 'iso': 'SY'},
            {'name': 'Tajikistan', 'iso': 'TJ'},
            {'name': 'Taiwan', 'iso': 'TW'},
            {'name': 'Thailand', 'iso': 'TH'},
            {'name': 'Timor-Leste', 'iso': 'TL'},
            {'name': 'Turkey', 'iso': 'TR'},
            {'name': 'Turkmenistan', 'iso': 'TM'},
            {'name': 'United Arab Emirates', 'iso': 'AE'},
            {'name': 'Uzbekistan', 'iso': 'UZ'},
            {'name': 'Vietnam', 'iso': 'VN'},
            {'name': 'Yemen', 'iso': 'YE'}
        ],
        'Europe': [
            {'name': 'Albania', 'iso': 'AL'},
            {'name': 'Andorra', 'iso': 'AD'},
            {'name': 'Austria', 'iso': 'AT'},
            {'name': 'Belarus', 'iso': 'BY'},
            {'name': 'Belgium', 'iso': 'BE'},
            {'name': 'Bosnia and Herzegovina', 'iso': 'BA'},
            {'name': 'Bulgaria', 'iso': 'BG'},
            {'name': 'Croatia', 'iso': 'HR'},
            {'name': 'Czechia', 'iso': 'CZ'},
            {'name': 'Cyprus', 'iso': 'CY'},
            {'name': 'Denmark', 'iso': 'DK'},
            {'name': 'Estonia', 'iso': 'EE'},
            {'name': 'Finland', 'iso': 'FI'},
            {'name': 'France', 'iso': 'FR'},
            {'name': 'Georgia', 'iso': 'GE'},
            {'name': 'Germany', 'iso': 'DE'},
            {'name': 'Greece', 'iso': 'GR'},
            {'name': 'Hungary', 'iso': 'HU'},
            {'name': 'Iceland', 'iso': 'IS'},
            {'name': 'Ireland', 'iso': 'IE'},
            {'name': 'Italy', 'iso': 'IT'},
            {'name': 'Kosovo', 'iso': 'XK'},
            {'name': 'Latvia', 'iso': 'LV'},
            {'name': 'Liechtenstein', 'iso': 'LI'},
            {'name': 'Lithuania', 'iso': 'LT'},
            {'name': 'Luxembourg', 'iso': 'LU'},
            {'name': 'Malta', 'iso': 'MT'},
            {'name': 'Moldova', 'iso': 'MD'},
            {'name': 'Monaco', 'iso': 'MC'},
            {'name': 'Montenegro', 'iso': 'ME'},
            {'name': 'Netherlands', 'iso': 'NL'},
            {'name': 'North Macedonia', 'iso': 'MK'},
            {'name': 'Norway', 'iso': 'NO'},
            {'name': 'Poland', 'iso': 'PL'},
            {'name': 'Portugal', 'iso': 'PT'},
            {'name': 'Romania', 'iso': 'RO'},
            {'name': 'Russia', 'iso': 'RU'},
            {'name': 'San Marino', 'iso': 'SM'},
            {'name': 'Serbia', 'iso': 'RS'},
            {'name': 'Slovakia', 'iso': 'SK'},
            {'name': 'Slovenia', 'iso': 'SI'},
            {'name': 'Spain', 'iso': 'ES'},
            {'name': 'Sweden', 'iso': 'SE'},
            {'name': 'Switzerland', 'iso': 'CH'},
            {'name': 'Ukraine', 'iso': 'UA'},
            {'name': 'United Kingdom', 'iso': 'GB'},
            {'name': 'Vatican City', 'iso': 'VA'}
        ],
        'North America': [
            {'name': 'Antigua and Barbuda', 'iso': 'AG'},
            {'name': 'Bahamas', 'iso': 'BS'},
            {'name': 'Barbados', 'iso': 'BB'},
            {'name': 'Belize', 'iso': 'BZ'},
            {'name': 'Canada', 'iso': 'CA'},
            {'name': 'Costa Rica', 'iso': 'CR'},
            {'name': 'Cuba', 'iso': 'CU'},
            {'name': 'Dominica', 'iso': 'DM'},
            {'name': 'Dominican Republic', 'iso': 'DO'},
            {'name': 'El Salvador', 'iso': 'SV'},
            {'name': 'Grenada', 'iso': 'GD'},
            {'name': 'Guatemala', 'iso': 'GT'},
            {'name': 'Haiti', 'iso': 'HT'},
            {'name': 'Honduras', 'iso': 'HN'},
            {'name': 'Jamaica', 'iso': 'JM'},
            {'name': 'Mexico', 'iso': 'MX'},
            {'name': 'Nicaragua', 'iso': 'NI'},
            {'name': 'Panama', 'iso': 'PA'},
            {'name': 'Saint Kitts and Nevis', 'iso': 'KN'},
            {'name': 'Saint Lucia', 'iso': 'LC'},
            {'name': 'Saint Vincent and the Grenadines', 'iso': 'VC'},
            {'name': 'Trinidad and Tobago', 'iso': 'TT'},
            {'name': 'United States', 'iso': 'US'}
        ],
        'Oceania': [
            {'name': 'Australia', 'iso': 'AU'},
            {'name': 'Fiji', 'iso': 'FJ'},
            {'name': 'Kiribati', 'iso': 'KI'},
            {'name': 'Marshall Islands', 'iso': 'MH'},
            {'name': 'Micronesia', 'iso': 'FM'},
            {'name': 'Nauru', 'iso': 'NR'},
            {'name': 'New Zealand', 'iso': 'NZ'},
            {'name': 'Palau', 'iso': 'PW'},
            {'name': 'Papua New Guinea', 'iso': 'PG'},
            {'name': 'Samoa', 'iso': 'WS'},
            {'name': 'Solomon Islands', 'iso': 'SB'},
            {'name': 'Tonga', 'iso': 'TO'},
            {'name': 'Tuvalu', 'iso': 'TV'},
            {'name': 'Vanuatu', 'iso': 'VU'}
        ],
        'South America': [
            {'name': 'Argentina', 'iso': 'AR'},
            {'name': 'Bolivia', 'iso': 'BO'},
            {'name': 'Brazil', 'iso': 'BR'},
            {'name': 'Chile', 'iso': 'CL'},
            {'name': 'Colombia', 'iso': 'CO'},
            {'name': 'Ecuador', 'iso': 'EC'},
            {'name': 'Guyana', 'iso': 'GY'},
            {'name': 'Paraguay', 'iso': 'PY'},
            {'name': 'Peru', 'iso': 'PE'},
            {'name': 'Suriname', 'iso': 'SR'},
            {'name': 'Uruguay', 'iso': 'UY'},
            {'name': 'Venezuela', 'iso': 'VE'}
        ]
    }


def main():
    print("Loading countries by continent from curated Wikipedia data...")
    print("Sources:")
    print("  - https://simple.wikipedia.org/wiki/List_of_countries_by_continents")
    print("  - https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2\n")

    countries_by_continent = get_countries_by_continent()

    # Load existing continents.json
    continents_data = {
        "af": {"name": "Africa"},
        "an": {"name": "Antarctica"},
        "as": {"name": "Asia"},
        "eu": {"name": "Europe"},
        "na": {"name": "North America"},
        "oc": {"name": "Oceania"},
        "sa": {"name": "South America"}
    }

    # Map continent names to codes
    continent_name_to_code = {v["name"]: k for k, v in continents_data.items()}

    # Update with country lists
    for continent_name, countries in countries_by_continent.items():
        code = continent_name_to_code.get(continent_name)
        if code:
            # Sort countries by name
            sorted_countries = sorted(countries, key=lambda x: x['name'])
            continents_data[code]["countries"] = sorted_countries

    # Display summary
    print("=" * 80)
    print("SUMMARY OF COUNTRIES BY CONTINENT WITH ISO CODES")
    print("=" * 80)

    total_countries = 0
    for code in sorted(continents_data.keys()):
        continent = continents_data[code]
        country_list = continent.get("countries", [])
        country_count = len(country_list)
        total_countries += country_count

        print(f"\n{continent['name']} ({code.upper()}): {country_count} countries")
        if country_count > 0:
            # Display all countries for small lists, preview for larger ones
            if country_count <= 10:
                for country in country_list:
                    print(f"  - {country['name']:40s} ({country['iso']})")
            else:
                # Display first 5 countries as preview
                preview = country_list[:5]
                for country in preview:
                    print(f"  - {country['name']:40s} ({country['iso']})")
                print(f"  ... (+{country_count - 5} more)")

    print("\n" + "=" * 80)
    print(f"Total countries: {total_countries}")
    print("=" * 80 + "\n")

    # Save updated data
    output_path = ".github/data/continents.json"
    with open(output_path, 'w') as f:
        json.dump(continents_data, f, indent=2, ensure_ascii=False)

    print(f"✓ Updated {output_path}")


if __name__ == "__main__":
    main()
