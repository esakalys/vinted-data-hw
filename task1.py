
from mapReduce import MapReduce

def map(clicks):
    return [
        {
            'key': click['date'],
            'value': 1
        }
        for click in clicks
    ]

def reduce(input):
    return [
        {
            "date": input['key'],
            "count": len(input['values'])
        }
    ]


def main():
    mapReduce = MapReduce(
        map={
            'data/clicks': map,
        },
        reduce=reduce,
        output='data/total_clicks'
    )

    mapReduce.run()


if __name__ == '__main__':
    main()
