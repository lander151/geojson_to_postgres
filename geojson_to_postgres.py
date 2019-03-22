import json
import geojson
from shapely.geometry import shape
from datetime import datetime
import psycopg2


class GeojsonToMysql:

    def __init__(self):
        self.cnx = psycopg2.connect(user='angular',
                                    password='angular',
                                    host='localhost',
                                    database='angular_app')
        self.cursor = self.cnx.cursor()

    def open_file(self, path):

        with open(path, 'r') as file_output:
            print('started at ' + str(datetime.now()))
            to_dict = json.loads(file_output.read())

            return to_dict['features']

    def parse_features(self, features_list):
        for feature in features_list:

            json_file = json.dumps(feature['geometry'])

            g1 = geojson.loads(json_file)

            g2 = shape(g1)

            feature_dict = (feature['properties'].get('NAME_0'), feature['properties'].get('NAME_1'),
                            feature['properties'].get('NAME_2'), feature['properties'].get('NAME_3'),
                            feature['properties'].get('NAME_4'), g2.wkt)
            self.insert_in_mysql(feature_dict)

    def insert_in_mysql(self, feature):
        sql = """
        INSERT INTO cities (country, region, province, county, city, geom) 
        VALUES (%s,%s,%s,%s,%s, (ST_GeomFromText(%s, 4326)))"""

        self.cursor.execute(sql, feature)
        self.cnx.commit()
        print("Inserted: " + feature[4])

    def __del__(self):
        self.cursor.close()
        self.cnx.close()


if __name__ == '__main__':
    src = "/home/lander/Escritorio/espana.geojson"
    geo = GeojsonToMysql()

    file = geo.open_file(src)
    geo.parse_features(file)

    print('finished at ' + str(datetime.now()))
