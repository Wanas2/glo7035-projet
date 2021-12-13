# GLO7035-Projet

Projet de vélo épicurien (Équipe 6)  
Liste des appels possibles

### @GET /heartbeat

returns:

    {
        "villeChoisie": str
    }

### @GET /extracted_data

returns:

    {
        "nbRestaurants":int,
        "nbSegments":int
    }

### @GET /transformed_data

returns:

    {
        "restaurants":{
            $type1: int,
            $type2: int,
            ...
        },
        "longueurCyclable":float
    }

### @GET /readme

### @GET /type

returns:

    [
        str,
        str,
        str,
        ...
    ]

### @GET /starting_point

(avec le payload):

    {
        "length": int (en mètre),
        "type": [str, str, ... ]
    }

returns:

    {
        "startingPoint" : {"type":"Point", "coordinates":[float, float]}
    }

### @GET /parcours

(avec le payload):

    {
        "startingPoint" : {"type":"Point", "coordinates":[float, float]},
        "length": int (en mètre),
        "numberOfStops": int,
        "type": [str, str, ... ]
    }

returns:

    {
        "type": "FeatureCollection",
        "features": [
        {
                "type":"Feature",
                "geometry":{
                    "type": "Point",
                    "coordinates":  [float, float]
                },
                "properties":{
                    "name":str,
                    "type":str
                }
            }, ..., {
                "type":"Feature",
                "geometry":{
                    "type": "MultiLineString",
                    "coordinates": [[
                        [float, float],  [float, float],  [float, float], ...
                        ]]
                },
                "properties":{
                    "length":float (en mètres)
                }
            }
        ]
    }