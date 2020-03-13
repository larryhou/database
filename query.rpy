import mysql.connector

from twisted.web.resource import Resource
from twisted.web.server import Request
import urllib.parse, json
import io

class TranslationQuery(Resource):
    def decode_uri(self, uri): # type:(str)->dict
        sep = uri.find('?')
        return self.decode_parameters(uri[sep + 1:]) if sep > 0 else {}

    @staticmethod
    def decode_parameters(v):
        data = {}
        for item in [x for x in v.split('&')]:
            sep = item.find('=')
            if sep <= 0: continue
            k, v = item[:sep], item[sep+1:]
            data[k] = urllib.parse.unquote(v)
        return data

    @staticmethod
    def query_database(options): # type: (dict)->bytes
        database = mysql.connector.connect(host='localhost',
                                           user='root',
                                           password='larryhou',
                                           database='language')
        cursor = database.cursor()
        cursor.execute('SET NAMES utf8')
        table_names = []
        if 'language' in options:
            table_names.append(options.get('language'))
        else:
            cursor.execute('SHOW TABLES')
            table_names = [x for x, in cursor.fetchall()]
        result = {}
        for column in ('label', 'chinese', 'translation'):
            if column in options:
                records = result[column] = []
                for name in table_names:
                    cursor.execute("SELECT * FROM {} WHERE {} LIKE '%{}%' COLLATE utf8_general_ci".format(name, column, options[column]))
                    for t in cursor.fetchall():
                        records.append('{} | {}'.format(name, ' | '.join(t)))
        response = json.dumps(result, ensure_ascii=False, indent=4).encode('utf-8')
        database.close()
        return response

    def render_GET(self, request): # type: (Request)->bytes
        return self.render_POST(request)

    def render_POST(self, request): # type: (Request)->bytes
        options = self.decode_uri(uri=request.uri.decode('utf-8'))
        content = request.content  # type: io.BytesIO
        if content:
            content.seek(0)
            options.update(self.decode_parameters(content.read().decode('utf-8')))
        print(request, options)
        return self.query_database(options) + b'\n'

resource = TranslationQuery()