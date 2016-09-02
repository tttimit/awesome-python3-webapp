import asyncio

from www.models import User, Blog, Comment

from www import my_orm


def test():
    loop = asyncio.get_event_loop()
    yield from my_orm.create_pool(loop, user='www-data', password='www-data', database='awesome')

    u = User(name='Test', email='test@example.com', password='123456789', image='about:blank')

    yield from u.save()



for x in test():
    pass