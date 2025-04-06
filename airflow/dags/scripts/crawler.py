from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd

class Crawler:
    season_map = {
        "2019/20": 274,
        "2020/21": 363,
        "2021/22": 418,
        "2022/23": 489,
        "2023/24": 578
    }

    def __init__(self, season):
        """Initialize WebDriver and open the Premier League table page."""
        self.url = f"https://www.premierleague.com/tables?co=1&se={self.season_map[season]}&ha=-1"
        self.driver = self._setup_driver()
        self.wait = WebDriverWait(self.driver, 20)

    def _setup_driver(self):
        """Setup WebDriver and navigate to the target URL."""
        path = "../chromedriver-linux64/chromedriver"
        service = Service(path)
        driver = webdriver.Chrome(service=service)
        driver.get(self.url)
        return driver

    def accept_cookies(self):
        """Accept the cookies popup if present."""
        try:
            accept_cookies = self.wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
            accept_cookies.click()
            print("Accepted cookies")
            time.sleep(2)
        except Exception:
            print("No cookies popup found, continuing...")

    def open_dropdown(self):
        """Open the gameweek selection dropdown."""
        try:
            dropdown_xpath = '//div[@class="current" and @data-dropdown-current="gameweekNumbers"]'
            dropdown = self.wait.until(EC.presence_of_element_located((By.XPATH, dropdown_xpath)))

            self.driver.execute_script("arguments[0].scrollIntoView();", dropdown)
            time.sleep(1)

            try:
                dropdown.click()
            except Exception:
                self.driver.execute_script("arguments[0].click();", dropdown)

            print("Opened game week dropdown")
            return dropdown_xpath  # Return dropdown XPath for later use
        except Exception as e:
            print(f"Error opening dropdown: {e}")

    def select_gameweek(self, game_week):
        """Select a specific game week from the dropdown."""
        try:
            game_week_xpath = f'//ul[@data-dropdown-list="gameweekNumbers"]/li[@data-option-name="{game_week}"]'
            game_week_option = self.wait.until(EC.presence_of_element_located((By.XPATH, game_week_xpath)))

            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", game_week_option)
            time.sleep(1)

            self.driver.execute_script("arguments[0].click();", game_week_option)
            print(f"Selected game week {game_week}")
            time.sleep(3)  # Wait for the table to update
        except Exception as e:
            print(f"Error selecting game week {game_week}: {e}")

    def get_table_data(self):
        """Scrape the first 20 rows of the Premier League table."""
        rows = self.driver.find_elements(By.TAG_NAME, "tr")
        position_arr = []
        club_arr = []
        abbreviation_arr = []
        played_arr = []
        won_arr = []
        drawn_arr = []
        lost_arr = []
        GF_arr = []
        GA_arr = []
        GD_arr = []
        points_arr = []
        form_arr = []


        for i, row in enumerate(rows):
            try:
                position = row.find_element(By.CLASS_NAME, "league-table__value").text  # Position
                club_element = row.find_element(By.XPATH, ".//span[@class='league-table__team-name league-table__team-name--long long']")
                club = club_element.get_attribute("textContent").strip() if club_element else "N/A"

                club_abbreviation_element = row.find_element(By.XPATH, ".//span[@class='league-table__team-name league-table__team-name--short short']")
                club_abbreviation = club_abbreviation_element.get_attribute("textContent").strip() if club_abbreviation_element else "N/A"

                tds = row.find_elements(By.TAG_NAME, "td")

                if len(tds) >= 9:
                    played, won, drawn, lost, gf, ga, gd = [td.text for td in tds[2:9]]
                    points = row.find_element(By.CLASS_NAME, "league-table__points").text

                    form_elements = row.find_elements(By.CLASS_NAME, "form-abbreviation")
                    form = "".join([element.get_attribute("textContent").strip() for element in form_elements]) if form_elements else "N/A"

                    position_arr.append(position)
                    club_arr.append(club)
                    abbreviation_arr.append(club_abbreviation)
                    played_arr.append(played)
                    won_arr.append(won)
                    drawn_arr.append(drawn)
                    lost_arr.append(lost)
                    GF_arr.append(gf)
                    GA_arr.append(ga)
                    GD_arr.append(gd)
                    points_arr.append(points)
                    form_arr.append(form)
                    if position=='20':
                        break

            except Exception as e:
                continue
        data = {"position":position_arr, "club":club_arr, "abbreviation":abbreviation_arr, "played":played_arr, "won": won_arr,
                "drawn":drawn_arr, "lost":lost_arr, "GF":GF_arr, "GA":GA_arr, "GD":GD_arr, "points":points_arr, "form":form_arr,
                }
        dataframe = pd.DataFrame(data)
        return dataframe

    def run(self, start_game_week=1, end_game_week=38):  # Default to full season
        """Run the scraping process for a given game week range."""
        try:
            self.accept_cookies()
            dropdown_xpath = self.open_dropdown()
            season_dataframe = []

            for game_week in range(start_game_week, end_game_week + 1):
                self.select_gameweek(game_week)
                table_data = self.get_table_data()

                table_data["game_week"] = game_week
                season_dataframe.append(table_data)

                dropdown = self.wait.until(EC.presence_of_element_located((By.XPATH, dropdown_xpath)))
                self.driver.execute_script("arguments[0].click();", dropdown)
                time.sleep(1)

            final_dataframe = pd.concat(season_dataframe, ignore_index=True)

        finally:
            self.driver.quit()
        return final_dataframe

# crawler = Crawler('2019/20')
# crawler.run(1,3)

