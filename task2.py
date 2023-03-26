
from mapReduce import MapReduce

def map_users(users):
    # Setting default column names
    value = {
        'id': None,
        'city': None,
        'country': None
        }
    result = []
    for user in users:
        if user['country'] == 'LT':
            value.update(user)
            result.append(
                {
                    'key': user['id'],
                    'value': {**value, 'table': 'users'}
                }
            )
    return result

def map_clicks(clicks):
    # Setting default column names
    value = {
        'date': None,
        'screen': None,
        'user_id': None,
        'click_target': None
        }
    result = []
    for click in clicks:
        value.update(click)
        result.append(
            {
                'key': click['user_id'],
                'value': {**value, 'table': 'clicks'}
            }
        )
    return result

def reduce(input):
    user = next((value for value in input['values'] if value['table'] == 'users'), None)
    results = []
    for click in input['values']:
        if click['table'] == 'clicks' and user is not None:
            result = {**click, **user}
            # Removing redundant columns
            del result['table']
            del result['id']
            results.append(result)

    return results

def main():
    mapReduce = MapReduce(
        map={
            'data/users': map_users,
            'data/clicks': map_clicks,
        },
        reduce=reduce,
        output='data/filtered_clicks'
    )

    mapReduce.run()


if __name__ == '__main__':
    main()
    