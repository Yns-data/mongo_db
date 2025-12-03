from dst_de_airlines_api.USE_CASES.insert_flight_uc import insert_flight


def insert_operation_fly(json_file):
    
    for flight in json_file['operationalFlights']:
        insert_flight(flight)







