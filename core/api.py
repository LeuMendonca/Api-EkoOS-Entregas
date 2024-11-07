from ninja import NinjaAPI
from api_entregas.api import api_entregas

api = NinjaAPI()

api.add_router('',api_entregas)