from datetime import date

from database import session
from models import PaymentType, Spectator, Performance, Actor, Role, Ticket


def fill_db():
    performances = [
        Performance(
            title="Crime and punishment", genre="Drama", duration=180,
            premier_date=date(2023, 9, 15), base_price=800.0, producer="Ivanov A.A.",
            age_restriction=16, is_scheduled=True
        ),
        Performance(
            title="Otello", genre="Tragedy", duration=150,
            premier_date=date(2023, 10, 5), base_price=1200.0, producer="Petrov B.B.",
            age_restriction=18, is_scheduled=True
        ),
        Performance(
            title="Thunder", genre="Drama", duration=120,
            premier_date=date(2023, 11, 20), base_price=600.0, producer="Sidorov C.C.",
            age_restriction=16, is_scheduled=True
        ),
        Performance(
            title="The Seagull", genre="Comedy", duration=135,
            premier_date=date(2024, 1, 10), base_price=900.0, producer="Chekhov A.P.",
            age_restriction=12, is_scheduled=True
        ),
        Performance(
            title="Hamlet", genre="Tragedy", duration=210,
            premier_date=date(2023, 12, 1), base_price=1500.0, producer="Shakespeare W.",
            age_restriction=16, is_scheduled=False
        ),
    ]

    session.add_all(performances)
    session.flush()

    actors = [
        Actor(
            name='Vinicyn A.A.', specialization='Drama', months_exp=12,
            birthday=date(2004, 12, 28), salary=120000.0, job_date=date(2024, 2, 26)
        ),
        Actor(
            name='Smirnova E.V.', specialization='Tragedy', months_exp=36,
            birthday=date(1990, 5, 15), salary=150000.0, job_date=date(2021, 3, 10)
        ),
        Actor(
            name='Kuznetsov P.P.', specialization='Comedy', months_exp=24,
            birthday=date(1995, 8, 22), salary=90000.0, job_date=date(2022, 6, 1)
        ),
        Actor(
            name='Morozova I.S.', specialization='Drama', months_exp=48,
            birthday=date(1988, 11, 3), salary=180000.0, job_date=date(2019, 9, 15)
        ),
    ]

    session.add_all(actors)
    session.flush()

    roles = [
        Role(
            performance_id=performances[0].id,  # Crime and punishment
            actor_id=actors[0].id,        # Vinicyn A.A.
            name='Raskolnikov', value=1.0
        ),
        Role(
            performance_id=performances[0].id,
            actor_id=actors[3].id,        # Morozova I.S.
            name='Sonya', value=0.8
        ),
        Role(
            performance_id=performances[1].id,  # Otello
            actor_id=actors[1].id,        # Smirnova E.V.
            name='Desdemona', value=0.9
        ),
        Role(
            performance_id=performances[2].id,  # Thunder
            actor_id=actors[2].id,        # Kuznetsov P.P.
            name='Boris', value=0.7
        ),
        Role(
            performance_id=performances[3].id,  # The Seagull
            actor_id=actors[2].id,        # Kuznetsov P.P.
            name='Treplev', value=0.6
        ),
    ]

    session.add_all(roles)
    session.flush()

    spectators = [
        Spectator(
            full_name="Winston Smith", email="warispeace@bigbrother.com", phone="79161231984",
            registration_date=date(2023, 1, 15), visits_count=5, last_visit_date=date(2024, 2, 10)
        ),
        Spectator(
            full_name="Kim Kitsuragi", email="revachol4ever@gmail.com", phone="79161238841",
        ),
        Spectator(
            full_name="Karl Marx", email="communist1828@inbox.com", phone="79161133741",
            registration_date=date(2022, 5, 5), visits_count=12, last_visit_date=date(2024, 2, 15)
        ),
        Spectator(
            full_name="Jean-Paul Sartre", email="existential@gmail.com", phone="79162234455",
            registration_date=date(2023, 8, 10), visits_count=2, last_visit_date=date(2023, 12, 5)
        ),
        Spectator(
            full_name="Simone de Beauvoir", email="feminism@inbox.com", phone="79163345566",
            registration_date=date(2023, 9, 1), visits_count=1, last_visit_date=date(2023, 9, 15)
        ),
    ]

    session.add_all(spectators)
    session.flush()

    tickets = [
        Ticket(
            spectator_id=spectators[0].id, performance_id=performances[0].id,
            seat_num=12, row=1, sector=1,
            pay_date=date(2023, 9, 10), price=800.0, is_used=True,
            payment_type=PaymentType.CASH,
        ),
        Ticket(
            spectator_id=spectators[1].id, performance_id=performances[1].id,
            seat_num=5, row=1, sector=2,
            pay_date=date(2023, 10, 1), price=1200.0, is_used=True,
            payment_type=PaymentType.CASH,
        ),
        Ticket(
            spectator_id=spectators[2].id, performance_id=performances[2].id,
            seat_num=4, row=3, sector=1,
            pay_date=date(2023, 11, 15), price=600.0, is_used=True
        ),
        Ticket(
            spectator_id=spectators[0].id, performance_id=performances[3].id,
            seat_num=8, row=2, sector=1,
            pay_date=date(2024, 1, 5), price=900.0,
        ),
        Ticket(
            spectator_id=spectators[3].id, performance_id=performances[0].id,
            seat_num=15, row=2, sector=3,
            pay_date=date(2023, 9, 8), price=800.0, is_used=True
        ),
        Ticket(
            spectator_id=spectators[4].id, performance_id=performances[3].id,
            seat_num=3, row=1, sector=1,
            pay_date=date(2024, 1, 8), price=900.0, is_used=True
        ),
        Ticket(
            spectator_id=spectators[0].id, performance_id=performances[4].id,
            seat_num=10, row=1, sector=1,
            pay_date=date(2023, 11, 25), price=1500.0,
        ),
    ]

    session.add_all(tickets)
    session.commit()
