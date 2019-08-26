import numpy

from flask import Flask, request, Response
import json
from models import *
import random
from sqlalchemy.ext.automap import automap_base

from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)

app = Flask(__name__)


@app.route('/imports', methods=['POST'])
def import_citizens():
    data = request.get_json()
    try:
        CitizenSchema(many=True).load(data["citizens"])
    except ValidationError as err:
        res = Response(json.dumps(err.messages), status=400)
        return res
    session = Session()
    metadata.reflect(bind=engine)
    import_id = (len(metadata.tables.keys()) + 1) * 100 + random.randint(0, 99)

    DB_cit, DB_rel, DB_association = create_citizens_collection('db' + str(import_id))
    Base.metadata.create_all(engine)
    id_set = set()
    for citizen in data["citizens"]:
        if citizen["citizen_id"] in id_set:
            res = Response("Duplicate citizen_ids", status=400)
            return res
        id_set.add(citizen["citizen_id"])
        try:
            newcitizen = DB_cit(citizen)
            if not (session.query(DB_rel).get(citizen["citizen_id"])):
                newrelative = DB_rel(citizen["citizen_id"])
                session.add(newrelative)
            for rel in citizen["relatives"]:
                if not (session.query(DB_rel).get(rel)):
                    newrelative = DB_rel(rel)
                else:
                    newrelative = session.query(DB_rel).get(rel)
                newcitizen.rel.append(newrelative)

            session.add(newcitizen)
            session.add(newrelative)

        except Exception as err:
            res = Response(str(err.args), status=400)
            return res
    try:
        session.commit()
    except Exception as err:
        res = Response(str(err.args), status=400)
        return res
    for cit in session.query(DB_cit).all():
        relatives_set_1 = set()
        relatives_set_2 = set()
        for elem in cit.rel:
            relatives_set_1.add(elem.citizen_id)
        for elem in session.query(DB_rel).get(cit.citizen_id).rel:
            relatives_set_2.add(elem.citizen_id)
        if relatives_set_1 != relatives_set_2:
            res = Response("Incorrect relatives", status=400)
            return res

    return_obj = {
        "data": {
            "import_id": import_id
        }
    }

    res = Response(json.dumps(return_obj), status=201)
    return res


@app.route('/imports/<int:import_id>/citizens/<int:citizen_id>', methods=['PATCH'])
def update_citizen(import_id, citizen_id):
    data = request.get_json()
    if data == {}:
        res = Response("Empty request", status=400)
        return res

    try:
        CitizenSchemaPatch(many=False).load(data)
    except ValidationError as err:
        res = Response(json.dumps(err.messages), status=400)
        return res
    session = Session()
    try:
        Base_ = automap_base(Base)
        Base_.prepare(engine, reflect=True)
        MyDB = Base_.classes['db' + str(import_id)]
        RelDB = Base_.classes["association_table_db" + str(import_id)]
        citizen = session.query(MyDB).get(citizen_id)
        for field in data:
            if field != "relatives":
                if field != "birth_date":
                    setattr(citizen, field, data[field])
                else:
                    setattr(citizen, field, datetime.datetime.strptime(data[field], '%d.%m.%Y'))
        if "relatives" in data:

            session.query(RelDB).filter_by(left_id=citizen_id).delete()
            session.query(RelDB).filter_by(right_id=citizen_id).delete()
            for r in data["relatives"]:
                first = RelDB()
                first.left_id = citizen_id
                first.right_id = r

                second = RelDB()
                second.left_id = r
                second.right_id = citizen_id

                session.add(first)
                session.add(second)
        session.commit()

        schema = CitizenSchema_out()
        citizen = session.query(MyDB).get(citizen_id)
        output = schema.dump(citizen)

        list_ = session.query(RelDB).filter_by(left_id=citizen_id).all()
        output["relatives"] = []
        for elem in list_:
            output["relatives"].append(elem.right_id)
        if citizen.birth_date.day < 10:
            output["birth_date"] = "0" + str(citizen.birth_date.day) + "."
        else:
            output["birth_date"] = str(citizen.birth_date.day) + "."
        if citizen.birth_date.month < 10:
            output["birth_date"] += "0" + str(citizen.birth_date.month)
        else:
            output["birth_date"] += str(citizen.birth_date.month)
        output["birth_date"] += "." + str(citizen.birth_date.year)
        output = {"data": output}

    except Exception as err:
        res = Response(str(err.args), status=400)
        return res
    res = Response(json.dumps(output, ensure_ascii=False).encode('utf8'), status=200)
    return res


@app.route('/imports/<int:import_id>/citizens', methods=['GET'])
def get_citizens_from_collection(import_id):
    try:
        session = Session()
        metadata.reflect(bind=engine)

        Base_ = automap_base(Base)
        Base_.prepare(engine, reflect=True)
        MyDB = Base_.classes['db' + str(import_id)]
        RelDB = Base_.classes["association_table_db" + str(import_id)]

        schema = CitizenSchema_out(many=True)
        citizen = session.query(MyDB).all()
        output = schema.dump(citizen)
        # print(output)
        for person in output:
            citizen = session.query(MyDB).get(person["citizen_id"])
            if citizen.birth_date.day < 10:
                person["birth_date"] = "0" + str(citizen.birth_date.day) + "."
            else:
                person["birth_date"] = str(citizen.birth_date.day) + "."
            if citizen.birth_date.month < 10:
                person["birth_date"] += "0" + str(citizen.birth_date.month)
            else:
                person["birth_date"] += str(citizen.birth_date.month)

            person["birth_date"] += "." + str(citizen.birth_date.year)
            list_ = session.query(RelDB).filter_by(left_id=person["citizen_id"]).all()
            person["relatives"] = []
            for elem in list_:
                person["relatives"].append(elem.right_id)
        output = {"data": output}

    except Exception as err:
        res = Response(str(err.args), status=400)
        return res
    res = Response(json.dumps(output, ensure_ascii=False).encode('utf8'), status=200)
    return res


@app.route('/imports/<int:import_id>/citizens/birthdays', methods=['GET'])
def count_presents(import_id):
    try:
        session = Session()
        metadata.reflect(bind=engine)

        Base_ = automap_base(Base)
        Base_.prepare(engine, reflect=True)
        MyDB = Base_.classes['db' + str(import_id)]
        RelDB = Base_.classes["association_table_db" + str(import_id)]
        months = [{} for i in range(12)]
        list_ = session.query(RelDB).all()
        for relation in list_:
            month = session.query(MyDB).get(relation.right_id).birth_date.month - 1
            if relation.left_id in months[month]:
                months[month][relation.left_id] += 1
            else:
                months[month][relation.left_id] = 1
        print(months)
        output = {"data": {}}
        for i in range(12):
            output["data"][str(i + 1)] = []
            print(months[i])
            for j in months[i]:
                output["data"][str(i + 1)].append({"citizen_id": j, "presents": months[i][j]})
    except Exception as err:
        res = Response(str(err.args), status=400)
        return res
    res = Response(json.dumps(output, ensure_ascii=False).encode('utf8'), status=200)
    return res


@app.route('/imports/<int:import_id>/towns/stat/percentile/age', methods=['GET'])
def calculate_percentile(import_id):
    try:
        session = Session()
        metadata.reflect(bind=engine)

        Base_ = automap_base(Base)
        Base_.prepare(engine, reflect=True)
        MyDB = Base_.classes['db' + str(import_id)]
        RelDB = Base_.classes["association_table_db" + str(import_id)]
        ages = {}
        list_ = session.query(MyDB).all()
        for person in list_:
            if person.town not in ages:
                ages[person.town] = []
            born = person.birth_date
            today = datetime.date.today()
            ages[person.town].append(today.year - born.year - ((today.month, today.day) < (born.month, born.day)))
        output = {"data": []}
        for town in ages:
            output["data"].append({"town": town, "p50": numpy.percentile(ages[town], 50, interpolation='linear'),
                                   "p75": numpy.percentile(ages[town], 75, interpolation='linear'),
                                   "p99": numpy.percentile(ages[town], 99, interpolation='linear')})
    except Exception as err:
        res = Response(str(err.args), status=400)
        return res
    res = Response(json.dumps(output, ensure_ascii=False).encode('utf8'), status=200)
    return res


app.run(host='0.0.0.0', port=8080, debug=True)
