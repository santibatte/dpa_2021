#from flask_chicago import Flask

import Flask
from flask_sqlalchemy import AQLAlchemy
from flask_restplus import Api, Resource, fields

from src.utils.utils import get_db_conn_sql_alchemy

#connect to db string
db_conn_str = get_db_conn_sql_alchemy('conf/local/credentials.yaml')

# create flask aoo
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = db_conn_str
api= Api(app)

#tabla
class Match(db.Model):
    __table_args__ = {'schema': 'dpa_storeapi'}
    __tablename__ = 'store_predictions'

    id_client = db.Column(db.Varchar, primary_key = True)
    prediction_date = db.Column(db.Varchar)
    model_label = db.Column(db.Varchar)
    score_label_0= db.Column(db.Varchar)
    score_label_1= db.Column(db.Varchar)

    def __repr__(self):
        return(u'<{self.__class__,__name__}: {self.id}>'.format(self=self))

#swagger model for marshalling outputs
model = api.model('store_predictions', {
    'id_client' : fields.Varchar,
    'prediction_date' : fields.Varchar,
    'model_label' : fields.Varchar,
    'score_label_0' : fields.Varchar,
    'score_label_1' : fields.Varchar
})

## output
#completar

## endpoints
@api.route('/')
class GoodbyeLili(Resource):
    def get(self):
        return {'Lili': "Lili disfrutamos mucho la materia gracias por todo"}

@api.route('/id_establecimiento/<int:id_client>')
class ShowPrediction(Resource):
    @api.marshall_with(model)
    def get(self,id_client):
        prediction = Match.query.filter_by(id_client=id_client)
        return prediction

@api.route('/prediction_date/<string:prediction_date>')
class PredictionDate(Resource):
    @api.marshall_with(model)
    def get(self,id_client):
        prediction = Match.query.filter_by(prediction_date=prediction_date)
        return prediction

if __name__ = '__main__':
    app.run(debug=True)
