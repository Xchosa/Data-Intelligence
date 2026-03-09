import sys
import requests
from dotenv import load_dotenv
import os
import json

load_dotenv()

class Parser():
    
    def __init__(self) -> None:
        """ init parser """
        self.base_Url = os.getenv("base_Url")
        self.headers = os.getenv("headers")
        self.endpoint = os.getenv("endpoint")


    def fetch_data(self):
        """ fetch data from API """
        try:
            full_url = f"{self.base_Url}{self.endpoint}"
            response = requests.get(full_url, headers=self.headers, timeout=5)
            print(response.status_code)

            with open("outfile.json", "w") as outfile:
                json.dump(response.json(), outfile, indent=2)
            print(response.json())
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching data: {e}")
            return None


class Visualizer():
    
    def __init__(self) -> None:
        """ init visualizer """
        pass


class Manager():
    
    def __init__(self, *, parser: Parser, visualizer: Visualizer) -> None:
        """ init manager with components """
        self.parser = parser
        self.visualizer = visualizer



if __name__ == "__main__":
    try:
        print("hello")
        parser = Parser()
        visualizer = Visualizer()
        manager = Manager(parser=parser, visualizer=visualizer)

        manager.parser.fetch_data()
        print("hello")

        

    except Exception as e:
        print(f"An error occurred: {e}")

