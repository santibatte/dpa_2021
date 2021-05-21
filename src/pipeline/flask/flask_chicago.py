#from flask_chicago import Flask

from flask import Flask
#from functools import cached_property
#from werkzeug.utils import cached_property
from flask_sqlalchemy import SQLAlchemy
from flask_restplus import Api, Resource, fields

from src.utils.utils import get_db_conn_sql_alchemy

#connect to db string
db_conn_str = get_db_conn_sql_alchemy('../../../conf/local/credentials.yaml')

# create flask aoo
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = db_conn_str
api= Api(app)

db=SQLAlchemy(app)

#tabla
class Match(db.Model):
    __table_args__ = {'schema': 'dpa_storeapi'}
    __tablename__ = 'store_predictions'

    id_client = db.Column(db.String, primary_key = True)
    prediction_date = db.Column(db.String)
    model_label = db.Column(db.String)
    score_label_0= db.Column(db.String)
    score_label_1= db.Column(db.String)

    def __repr__(self):
        return(u'<{self.__class__,__name__}: {self.id}>'.format(self=self))

#swagger model for marshalling outputs
model = api.model('store_predictions', {
    'id_client' : fields.String,
    'prediction_date' : fields.String,
    'model_label' : fields.String,
    'score_label_0' : fields.String,
    'score_label_1' : fields.String
})

## output
#completar

## endpoints
@api.route('/')
class GoodbyeLili(Resource):
    def get(self):
        return {'Lili': "Lili disfrutamos mucho la materia gracias por todo"}

@api.route('/id_establecimiento/<string:id_client>')
class ShowPrediction(Resource):
    @api.marshall_with(model)
    def get(self,id_client):
        prediction = Match.query.filter_by(id_client=id_client).all()
        return prediction

@api.route('/prediction_date/<string:prediction_date>')
class PredictionDate(Resource):
    @api.marshal_with(model)
    def get(self,prediction_date):
        prediction = Match.query.filter_by(prediction_date=prediction_date).all()
        return prediction

if __name__ == '__main__':
    app.run(debug=True)
