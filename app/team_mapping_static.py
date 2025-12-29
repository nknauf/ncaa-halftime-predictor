"""
Hardcoded ESPN displayName → Sports-Reference team slug.

Contract:
- Input must be EXACT ESPN displayName (school + mascot).
- Output is sportsref_id (slug).
- Raises KeyError if unmapped.
"""

ESPN_TO_SPORTSREF = {
    # =====================
    # ACC
    # =====================
    "Boston College Eagles": "boston-college",
    "California Golden Bears": "california",
    "Clemson Tigers": "clemson",
    "Duke Blue Devils": "duke",
    "Florida State Seminoles": "florida-state",
    "Georgia Tech Yellow Jackets": "georgia-tech",
    "Louisville Cardinals": "louisville",
    "Miami Hurricanes": "miami-fl",
    "NC State Wolfpack": "north-carolina-state",
    "North Carolina Tar Heels": "north-carolina",
    "Notre Dame Fighting Irish": "notre-dame",
    "Pittsburgh Panthers": "pittsburgh",
    "SMU Mustangs": "southern-methodist",
    "Stanford Cardinal": "stanford",
    "Syracuse Orange": "syracuse",
    "Virginia Cavaliers": "virginia",
    "Virginia Tech Hokies": "virginia-tech",
    "Wake Forest Demon Deacons": "wake-forest",

    # =====================
    # ASUN
    # =====================
    "Austin Peay Governors": "austin-peay",
    "Bellarmine Knights": "bellarmine",
    "Central Arkansas Bears": "central-arkansas",
    "Eastern Kentucky Colonels": "eastern-kentucky",
    "Florida Gulf Coast Eagles": "florida-gulf-coast",
    "Jacksonville Dolphins": "jacksonville",
    "Lipscomb Bisons": "lipscomb",
    "North Alabama Lions": "north-alabama",
    "North Florida Ospreys": "north-florida",
    "Queens University Royals": "queens-nc",
    "Stetson Hatters": "stetson",

    # =====================
    # America East
    # =====================
    "Binghamton Bearcats": "binghamton",
    "Bryant Bulldogs": "bryant",
    "Maine Black Bears": "maine",
    "NJIT Highlanders": "njit",
    "New Hampshire Wildcats": "new-hampshire",
    "UAlbany Great Danes": "albany-ny",
    "UMBC Retrievers": "maryland-baltimore-county",
    "UMass Lowell River Hawks": "massachusetts-lowell",
    "Vermont Catamounts": "vermont",

    # =====================
    # American
    # =====================
    "Charlotte 49ers": "charlotte",
    "East Carolina Pirates": "east-carolina",
    "Florida Atlantic Owls": "florida-atlantic",
    "Memphis Tigers": "memphis",
    "North Texas Mean Green": "north-texas",
    "Rice Owls": "rice",
    "South Florida Bulls": "south-florida",
    "Temple Owls": "temple",
    "Tulane Green Wave": "tulane",
    "Tulsa Golden Hurricane": "tulsa",
    "UAB Blazers": "alabama-birmingham",
    "UTSA Roadrunners": "texas-san-antonio",
    "Wichita State Shockers": "wichita-state",

    # =====================
    # Atlantic 10
    # =====================
    "Davidson Wildcats": "davidson",
    "Dayton Flyers": "dayton",
    "Duquesne Dukes": "duquesne",
    "Fordham Rams": "fordham",
    "George Mason Patriots": "george-mason",
    "George Washington Revolutionaries": "george-washington",
    "La Salle Explorers": "la-salle",
    "Loyola Chicago Ramblers": "loyola-il",
    "Rhode Island Rams": "rhode-island",
    "Richmond Spiders": "richmond",
    "Saint Joseph's Hawks": "saint-josephs",
    "Saint Louis Billikens": "saint-louis",
    "St. Bonaventure Bonnies": "st-bonaventure",
    "VCU Rams": "virginia-commonwealth",

    # =====================
    # Big 12
    # =====================
    "Arizona State Sun Devils": "arizona-state",
    "Arizona Wildcats": "arizona",
    "BYU Cougars": "brigham-young",
    "Baylor Bears": "baylor",
    "Cincinnati Bearcats": "cincinnati",
    "Colorado Buffaloes": "colorado",
    "Houston Cougars": "houston",
    "Iowa State Cyclones": "iowa-state",
    "Kansas Jayhawks": "kansas",
    "Kansas State Wildcats": "kansas-state",
    "Oklahoma State Cowboys": "oklahoma-state",
    "TCU Horned Frogs": "texas-christian",
    "Texas Tech Red Raiders": "texas-tech",
    "UCF Knights": "central-florida",
    "Utah Utes": "utah",
    "West Virginia Mountaineers": "west-virginia",

    # =====================
    # Big East
    # =====================
    "Butler Bulldogs": "butler",
    "Creighton Bluejays": "creighton",
    "DePaul Blue Demons": "depaul",
    "Georgetown Hoyas": "georgetown",
    "Marquette Golden Eagles": "marquette",
    "Providence Friars": "providence",
    "Seton Hall Pirates": "seton-hall",
    "St. John's Red Storm": "st-johns-ny",
    "UConn Huskies": "connecticut",
    "Villanova Wildcats": "villanova",
    "Xavier Musketeers": "xavier",

    # =====================
    # Big Sky
    # =====================
    "Eastern Washington Eagles": "eastern-washington",
    "Idaho State Bengals": "idaho-state",
    "Idaho Vandals": "idaho",
    "Montana Grizzlies": "montana",
    "Montana State Bobcats": "montana-state",
    "Northern Arizona Lumberjacks": "northern-arizona",
    "Northern Colorado Bears": "northern-colorado",
    "Portland State Vikings": "portland-state",
    "Sacramento State Hornets": "sacramento-state",
    "Weber State Wildcats": "weber-state",

    # =====================
    # Big South
    # =====================
    "Charleston Southern Buccaneers": "charleston-southern",
    "Gardner-Webb Runnin' Bulldogs": "gardner-webb",
    "High Point Panthers": "high-point",
    "Longwood Lancers": "longwood",
    "Presbyterian Blue Hose": "presbyterian",
    "Radford Highlanders": "radford",
    "South Carolina Upstate Spartans": "south-carolina-upstate",
    "UNC Asheville Bulldogs": "north-carolina-asheville",
    "Winthrop Eagles": "winthrop",

    # =====================
    # Big Ten
    # =====================
    "Illinois Fighting Illini": "illinois",
    "Indiana Hoosiers": "indiana",
    "Iowa Hawkeyes": "iowa",
    "Maryland Terrapins": "maryland",
    "Michigan State Spartans": "michigan-state",
    "Michigan Wolverines": "michigan",
    "Minnesota Golden Gophers": "minnesota",
    "Nebraska Cornhuskers": "nebraska",
    "Northwestern Wildcats": "northwestern",
    "Ohio State Buckeyes": "ohio-state",
    "Oregon Ducks": "oregon",
    "Penn State Nittany Lions": "penn-state",
    "Purdue Boilermakers": "purdue",
    "Rutgers Scarlet Knights": "rutgers",
    "UCLA Bruins": "ucla",
    "USC Trojans": "southern-california",
    "Washington Huskies": "washington",
    "Wisconsin Badgers": "wisconsin",

    # =====================
    # Big West
    # =====================
    "Cal Poly Mustangs": "cal-poly",
    "Cal State Bakersfield Roadrunners": "cal-state-bakersfield",
    "Cal State Fullerton Titans": "cal-state-fullerton",
    "Cal State Northridge Matadors": "cal-state-northridge",
    "Hawai'i Rainbow Warriors": "hawaii",
    "Long Beach State Beach": "long-beach-state",
    "UC Davis Aggies": "california-davis",
    "UC Irvine Anteaters": "california-irvine",
    "UC Riverside Highlanders": "california-riverside",
    "UC San Diego Tritons": "california-san-diego",
    "UC Santa Barbara Gauchos": "california-santa-barbara",

    # =====================
    # Mountain West
    # =====================
    "Air Force Falcons": "air-force",
    "Boise State Broncos": "boise-state",
    "Colorado State Rams": "colorado-state",
    "Fresno State Bulldogs": "fresno-state",
    "Grand Canyon Lopes": "grand-canyon",
    "Nevada Wolf Pack": "nevada",
    "New Mexico Lobos": "new-mexico",
    "San Diego State Aztecs": "san-diego-state",
    "San José State Spartans": "san-jose-state",
    "UNLV Rebels": "nevada-las-vegas",
    "Utah State Aggies": "utah-state",
    "Wyoming Cowboys": "wyoming",

    # =====================
    # Northeast
    # =====================
    "Central Connecticut Blue Devils": "central-connecticut-state",
    "Chicago State Cougars": "chicago-state",
    "Fairleigh Dickinson Knights": "fairleigh-dickinson",
    "Le Moyne Dolphins": "le-moyne",
    "Long Island University Sharks": "long-island-university",
    "Saint Francis Red Flash": "saint-francis-pa",
    "Stonehill Skyhawks": "stonehill",
    "Wagner Seahawks": "wagner",

    # =====================
    # Ohio Valley
    # =====================
    "Eastern Illinois Panthers": "eastern-illinois",
    "Lindenwood Lions": "lindenwood",
    "Little Rock Trojans": "arkansas-little-rock",
    "Morehead State Eagles": "morehead-state",
    "SIU Edwardsville Cougars": "southern-illinois-edwardsville",
    "Southeast Missouri State Redhawks": "southeast-missouri-state",
    "Southern Indiana Screaming Eagles": "southern-indiana",
    "Tennessee State Tigers": "tennessee-state",
    "Tennessee Tech Golden Eagles": "tennessee-tech",
    "UT Martin Skyhawks": "tennessee-martin",
    "Western Illinois Leathernecks": "western-illinois",

    # =====================
    # Patriot League
    # =====================
    "American University Eagles": "american",
    "Army Black Knights": "army",
    "Boston University Terriers": "boston-university",
    "Bucknell Bison": "bucknell",
    "Colgate Raiders": "colgate",
    "Holy Cross Crusaders": "holy-cross",
    "Lafayette Leopards": "lafayette",
    "Lehigh Mountain Hawks": "lehigh",
    "Loyola Maryland Greyhounds": "loyola-md",
    "Navy Midshipmen": "navy",

    # =====================
    # SEC
    # =====================
    "Alabama Crimson Tide": "alabama",
    "Arkansas Razorbacks": "arkansas",
    "Auburn Tigers": "auburn",
    "Florida Gators": "florida",
    "Georgia Bulldogs": "georgia",
    "Kentucky Wildcats": "kentucky",
    "LSU Tigers": "louisiana-state",
    "Mississippi State Bulldogs": "mississippi-state",
    "Missouri Tigers": "missouri",
    "Oklahoma Sooners": "oklahoma",
    "Ole Miss Rebels": "mississippi",
    "South Carolina Gamecocks": "south-carolina",
    "Tennessee Volunteers": "tennessee",
    "Texas A&M Aggies": "texas-am",
    "Texas Longhorns": "texas",
    "Vanderbilt Commodores": "vanderbilt",

    # =====================
    # Sun Belt
    # =====================
    "App State Mountaineers": "appalachian-state",
    "Arkansas State Red Wolves": "arkansas-state",
    "Coastal Carolina Chanticleers": "coastal-carolina",
    "Georgia Southern Eagles": "georgia-southern",
    "Georgia State Panthers": "georgia-state",
    "James Madison Dukes": "james-madison",
    "Louisiana Ragin' Cajuns": "louisiana-lafayette",
    "Marshall Thundering Herd": "marshall",
    "Old Dominion Monarchs": "old-dominion",
    "South Alabama Jaguars": "south-alabama",
    "Southern Miss Golden Eagles": "southern-mississippi",
    "Texas State Bobcats": "texas-state",
    "Troy Trojans": "troy",
    "UL Monroe Warhawks": "louisiana-monroe",

    # =====================
    # WAC
    # =====================
    "Abilene Christian Wildcats": "abilene-christian",
    "California Baptist Lancers": "california-baptist",
    "Southern Utah Thunderbirds": "southern-utah",
    "Tarleton State Texans": "tarleton-state",
    "UT Arlington Mavericks": "texas-arlington",
    "Utah Tech Trailblazers": "dixie-state",
    "Utah Valley Wolverines": "utah-valley",

    # =====================
    # West Coast
    # =====================
    "Gonzaga Bulldogs": "gonzaga",
    "Loyola Marymount Lions": "loyola-marymount",
    "Oregon State Beavers": "oregon-state",
    "Pacific Tigers": "pacific",
    "Pepperdine Waves": "pepperdine",
    "Portland Pilots": "portland",
    "Saint Mary's Gaels": "saint-marys-ca",
    "San Diego Toreros": "san-diego",
    "San Francisco Dons": "san-francisco",
    "Santa Clara Broncos": "santa-clara",
    "Seattle U Redhawks": "seattle",
    "Washington State Cougars": "washington-state",

    # =====================
    # MAAC
    # =====================
    "Canisius Golden Griffins": "canisius",
    "Fairfield Stags": "fairfield",
    "Iona Gaels": "iona",
    "Manhattan Jaspers": "manhattan",
    "Marist Red Foxes": "marist",
    "Merrimack Warriors": "merrimack",
    "Mount St. Mary's Mountaineers": "mount-st-marys",
    "Niagara Purple Eagles": "niagara",
    "Quinnipiac Bobcats": "quinnipiac",
    "Rider Broncs": "rider",
    "Sacred Heart Pioneers": "sacred-heart",
    "Saint Peter's Peacocks": "saint-peters",
    "Siena Saints": "siena",

    # =====================
    # MEAC
    # =====================
    "Coppin State Eagles": "coppin-state",
    "Delaware State Hornets": "delaware-state",
    "Howard Bison": "howard",
    "Maryland Eastern Shore Hawks": "maryland-eastern-shore",
    "Morgan State Bears": "morgan-state",
    "Norfolk State Spartans": "norfolk-state",
    "North Carolina Central Eagles": "north-carolina-central",
    "South Carolina State Bulldogs": "south-carolina-state",

    # =====================
    # SWAC
    # =====================
    "Alabama A&M Bulldogs": "alabama-am",
    "Alabama State Hornets": "alabama-state",
    "Alcorn State Braves": "alcorn-state",
    "Arkansas-Pine Bluff Golden Lions": "arkansas-pine-bluff",
    "Bethune-Cookman Wildcats": "bethune-cookman",
    "Florida A&M Rattlers": "florida-am",
    "Grambling Tigers": "grambling",
    "Jackson State Tigers": "jackson-state",
    "Mississippi Valley State Delta Devils": "mississippi-valley-state",
    "Prairie View A&M Panthers": "prairie-view",
    "Southern Jaguars": "southern",
    "Texas Southern Tigers": "texas-southern",

    # =====================
    # Missouri Valley
    # =====================
    "Belmont Bruins": "belmont",
    "Bradley Braves": "bradley",
    "Drake Bulldogs": "drake",
    "Evansville Purple Aces": "evansville",
    "Illinois State Redbirds": "illinois-state",
    "Indiana State Sycamores": "indiana-state",
    "Murray State Racers": "murray-state",
    "Northern Iowa Panthers": "northern-iowa",
    "Southern Illinois Salukis": "southern-illinois",
    "UIC Flames": "illinois-chicago",
    "Valparaiso Beacons": "valparaiso",

    # =====================
    # Horizon
    # =====================
    "Cleveland State Vikings": "cleveland-state",
    "Detroit Mercy Titans": "detroit-mercy",
    "Green Bay Phoenix": "green-bay",
    "IU Indianapolis Jaguars": "iupui",
    "Milwaukee Panthers": "milwaukee",
    "Northern Kentucky Norse": "northern-kentucky",
    "Oakland Golden Grizzlies": "oakland",
    "Purdue Fort Wayne Mastodons": "ipfw",
    "Robert Morris Colonials": "robert-morris",
    "Wright State Raiders": "wright-state",
    "Youngstown State Penguins": "youngstown-state",
}


def get_sports_reference_name(espn_team_name: str) -> str:
    """
    Convert exact ESPN displayName (school + mascot)
    to Sports-Reference slug.

    Raises KeyError if unmapped.
    """
    return ESPN_TO_SPORTSREF[espn_team_name]