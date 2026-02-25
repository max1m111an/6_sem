from datetime import date
from sqlalchemy import func, desc

from database import session
from models import Actor, Role, Performance, Spectator, Ticket

def clear_database():
    with session:
        session.query(Ticket).delete()
        session.query(Role).delete()
        session.query(Performance).delete()
        session.query(Actor).delete()
        session.query(Spectator).delete()
        session.commit()
    print("Database is clear.")


def find_performances_by_genre(genre_name):
    """1. Найти спектакли определенного жанра"""

    performances = session.query(Performance).filter(
        Performance.genre == genre_name,
        Performance.is_scheduled == True
    ).all()
    
    print(f"Performances by genre '{genre_name}':")
    for p in performances:
        print(f"  - {p.title} ({p.premier_date}) - {p.duration} mins")
    return performances

 
def show_performances_sorted_by_date():
    """2. Показать спектакли, отсортированные по дате показа"""

    performances = session.query(Performance).filter(
        Performance.is_scheduled == True
    ).order_by(Performance.premier_date).all()
    
    print("Performances by date:")
    for p in performances:
        status = "Y" if p.is_scheduled else "N"
        print(f"  {status} {p.premier_date.strftime('%d.%m.%Y')} - {p.title} ({p.genre})")
    return performances


def count_tickets_per_performance():
    """3. Посчитать количество проданных билетов на каждый спектакль"""

    results = session.query(
        Performance.title,
        Performance.premier_date,
        func.count(Ticket.id).label('tickets_sold'),
        func.sum(Ticket.price).label('total_revenue')
    ).outerjoin(Ticket, Performance.id == Ticket.performance_id
    ).filter(Performance.is_scheduled == True
    ).group_by(Performance.id, Performance.title, Performance.premier_date
    ).order_by(desc('tickets_sold')).all()
    
    print("Count saled ticktes by perfs:")
    for title, date, sold, revenue in results:
        print(f"  - {title} ({date.strftime('%d.%m.%Y')}): {sold} tickets, revenue: {revenue}")
    return results


def show_tickets_with_details():
    """4. Вывести билеты с информацией о зрителе и спектакле"""

    tickets = session.query(
        Ticket.id,
        Spectator.full_name.label('spectator'),
        Performance.title.label('performance'),
        Ticket.seat_num,
        Ticket.row,
        Ticket.sector,
        Ticket.price,
        Ticket.is_used,
        Ticket.pay_date
    ).join(Spectator, Ticket.spectator_id == Spectator.id
    ).join(Performance, Ticket.performance_id == Performance.id
    ).order_by(Ticket.pay_date.desc()).all()
    
    print("Tickets info:")
    for t in tickets:
        used = "Y" if t.is_used else "N"
        print(f"  Ticket #{t.id}: {t.performance} - {t.spectator}")
        print(f"    Seat: row {t.row}, seat_num {t.seat_num}, sector {t.sector}")
        print(f"    Price: {t.price}, pay_date: {t.pay_date}, used: {used}")
    return tickets


def find_performances_with_few_tickets(threshold=5):
    """5. Найти спектакли с количеством билетов меньше 5"""

    results = session.query(
        Performance.title,
        Performance.premier_date,
        func.count(Ticket.id).label('tickets_sold')
    ).outerjoin(Ticket, Performance.id == Ticket.performance_id
    ).group_by(Performance.id, Performance.title, Performance.premier_date
    ).having(func.count(Ticket.id) < threshold
    ).order_by('tickets_sold').all()
    
    print(f"Perfs with fewer than {threshold} tickets:")
    for title, date, sold in results:
        print(f"  - {title} ({date.strftime('%d.%m.%Y')}): {sold} tickets")
    return results

 
def find_active_spectators(min_tickets=3):
    """6. Найти зрителей, которые купили билеты на более чем 3 спектакля"""

    results = session.query(
        Spectator.full_name,
        func.count(Ticket.id).label('tickets_bought'),
        func.sum(Ticket.price).label('total_spent')
    ).join(Ticket, Spectator.id == Ticket.spectator_id
    ).group_by(Spectator.id, Spectator.full_name
    ).having(func.count(Ticket.id) >= min_tickets
    ).order_by(desc('tickets_bought')).all()
    
    print(f"Spectators, that bought min {min_tickets} tickets:")
    for name, tickets, spent in results:
        print(f"  - {name}: {tickets} tickets, spent: {spent}")
    return results

def update_vip_prices(vip_sectors=[1], multiplier=1.5):
    """7. Обновить цену билета для VIP мест"""

    vip_tickets = session.query(Ticket).filter(
        Ticket.sector.in_(vip_sectors),
        Ticket.is_used == False
    ).all()
    
    print(f"Updated prices for VIP sectors {vip_sectors}:")
    for ticket in vip_tickets:
        old_price = ticket.price
        ticket.price = round(old_price * multiplier, 2)
        print(f"  Ticket #{ticket.id}: price {old_price} -> {ticket.price}")
    
    session.commit()
    print(f"Updated {len(vip_tickets)} tickets")


def delete_canceled_performances():
    """8. Удалить отмененные спектакли"""

    # Canceled performances
    canceled = session.query(Performance).filter(
        Performance.is_scheduled == False
    ).all()
    
    print(f"Found canceled perfs: {len(canceled)}")
    
    for performance in canceled:
        # Del relations
        tickets_deleted = session.query(Ticket).filter(
            Ticket.performance_id == performance.id
        ).delete()
        
        print(f"  Performance '{performance.title}': del {tickets_deleted} tickets")
        session.delete(performance)
    
    session.commit()
    print("Canceled perfs removed")


def sell_ticket(spectator_id, performance_id, seat_num, row, sector, price, payment_type):
    """9. Продать билет"""

    # Performance is exists and not canceled
    performance = session.query(Performance).filter(
        Performance.id == performance_id,
        Performance.is_scheduled == True
    ).first()
    
    if not performance:
        print(f"Err: Performance #{performance_id} not found or canceled")
        return None
    
    # Seat is free
    existing = session.query(Ticket).filter(
        Ticket.performance_id == performance_id,
        Ticket.seat_num == seat_num,
        Ticket.row == row,
        Ticket.sector == sector
    ).first()
    
    if existing:
        print(f"Err: Seat row {row}, number {seat_num}, sector {sector} is occupied")
        return None
    
    new_ticket = Ticket(
        spectator_id=spectator_id,
        performance_id=performance_id,
        seat_num=seat_num,
        row=row,
        sector=sector,
        price=price,
        payment_type=payment_type,
    )
    
    session.add(new_ticket)
    
    spectator = session.query(Spectator).get(spectator_id)
    if spectator:
        spectator.visits_count += 1
        spectator.last_visit_date = date.today()
    
    session.commit()
    
    print(f"Ticket created!")
    print(f"  Performance: {performance.title}")
    print(f"  Spectator: {spectator.full_name}")
    print(f"  Seat: row {row}, num {seat_num}, sector {sector}")
    print(f"  Price: {price}")
    
    return new_ticket
# sell_ticket(spectator_id=1, performance_id=2, seat_num=10, row=2, sector=1, price=1500.0)


def analyze_actor_popularity():
    """
    Сложный запрос: анализ популярности актеров по жанрам
    Выводит для каждого актера: 
    - сколько спектаклей
    - общая выручка от билетов на эти спектакли
    - средняя важность роли
    """
    results = session.query(
        Actor.name,
        func.count(Performance.id).label('performances_count'),
        func.avg(Role.value).label('avg_importance'),
        func.sum(Ticket.price).label('total_revenue'),
    ).join(Role, Actor.id == Role.actor_id
    ).join(Performance, Role.performance_id == Performance.id
    ).outerjoin(Ticket, Performance.id == Ticket.performance_id
    ).filter(Performance.is_scheduled == True
    ).group_by(Actor.id, Actor.name
    ).order_by(Actor.name, desc('total_revenue')).all()
    
    print("Actors fame analysis:")    
    current_actor = None
    for name, perf_cnt, avg_imp, revenue in results:
        if name != current_actor:
            if current_actor:
                print()
            print(f"Actor: {name}")
            current_actor = name
        
        print(f"    Perfs: {perf_cnt}")
        print(f"    Avg value: {avg_imp:.2f}")
        print(f"    Revenue: {revenue}")
    
    return results
