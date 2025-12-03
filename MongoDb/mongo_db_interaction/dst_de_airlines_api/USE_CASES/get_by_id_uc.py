from dst_de_airlines_api.DAO.flights import get_by_id 

def get_flight_by_id(collection_name, id):
    flight = get_by_id(collection_name, id)
    return flight