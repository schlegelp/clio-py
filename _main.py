import clio

def main():
    query = '{"bodyid":[154109,24053]}'
    status_code, content = clio.post('prod', 'vnc-annotations-query', str_payload = query)
    if status_code != 200:
        print(f"Error in query request: {status_code}: {content}")
    else:
        print(f"Query result: {content}")

if __name__ == '__main__':
    main()