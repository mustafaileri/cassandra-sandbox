from cassandra_model import Cassandra

cassandra = Cassandra()
data = [
    dict(
        name='Mustafa',
        surname='İleri',
        title='Head of Data Engineering',
        skills={
            'first': 'skill one',
            'second': 'skill two',
            'third': 'skill three'},
        city='İstanbul'
    ),
    dict(
        name='Cengizhan',
        surname='Çalışkan',
        title='Data Engineer',
        skills={
            'first': 'skill one',
            'second': 'skill two',
            'third': 'skill three'},
        city='İstanbul'
    ),
    dict(
        name='Alpay',
        surname='Önal',
        title='Data Engineer',
        skills={
            'first': 'skill one',
            'second': 'skill two',
            'third': 'skill three'},
        city='İstanbul'
    ),
    dict(
        name='Sercan',
        surname='Doğan',
        title='Data Engineer',
        skills={
            'first': 'skill one',
            'second': 'skill two',
            'third': 'skill three'},
        city='İstanbul'
    ),
]

for developer in data:
    cassandra.write(developer)
