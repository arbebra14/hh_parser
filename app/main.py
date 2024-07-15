import chardet
import os
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
from typing import List
from pydantic import BaseModel
import logging
from logging.handlers import RotatingFileHandler

log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
log_file = 'app.log'

handler = RotatingFileHandler(log_file, maxBytes=10000000, backupCount=5, encoding='utf-8')
handler.setFormatter(log_formatter)
handler.setLevel(logging.INFO)

app_logger = logging.getLogger()
app_logger.setLevel(logging.INFO)
app_logger.addHandler(handler)

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class Vacancy(Base):
    __tablename__ = 'vacancies'
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String)
    description = Column(String)
    skills = Column(String)
    employment_format = Column(String)
    salary = Column(Integer, nullable=True)

class Applicant(Base):
    __tablename__ = 'applicants'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    skills = Column(String)

def create_tables():
    try:
        print("Creating tables...")
        Base.metadata.create_all(bind=engine)
        print("Tables created successfully!")
    except Exception as e:
        print(f"Error creating tables: {e}")

class VacancyBase(BaseModel):
    title: str
    description: str
    skills: str
    employment_format: str

class VacancyCreate(VacancyBase):
    pass

class VacancyResponse(VacancyBase):
    id: int

    class Config:
        from_attributes = True

class ApplicantBase(BaseModel):
    name: str
    skills: str

class ApplicantCreate(ApplicantBase):
    pass

class ApplicantResponse(ApplicantBase):
    id: int

    class Config:
        from_attributes = True

app = FastAPI()

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/vacancies", response_model=List[VacancyResponse])
def get_vacancies(query: str, db: Session = Depends(get_db)):
    url = f'https://hh.ru/search/vacancy?text={query}'
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    app_logger.info(f"Fetching data from: {url}")
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        app_logger.error(f"Error fetching data from hh.ru: {response.status_code}")
        raise HTTPException(status_code=response.status_code, detail="Error fetching data from hh.ru")
    encoding = 'utf-8'
    decoded_content = response.content.decode(encoding)

    app_logger.info(f"Response status code: {response.status_code}")
    app_logger.info(f"Response content length: {len(response.text)}")
    app_logger.info(f"Response content preview: {response.content[:500]}")

    encoding = chardet.detect(response.content)['encoding']
    app_logger.info(f"Detected encoding: {encoding}")

    decoded_content = response.content.decode(encoding)

    with open('page_content.html', 'w', encoding='utf-8') as file:
        file.write(decoded_content)

    soup = BeautifulSoup(decoded_content, 'html.parser')
    app_logger.info(f"Soup object created: {soup.prettify()[:500]}")

    vacancies = []
    for vacancy in soup.select('.vacancy-serp-item'):
        title = vacancy.select_one('.vacancy-serp-item__title').text if vacancy.select_one('.vacancy-serp-item__title') else None
        description = vacancy.select_one('.vacancy-serp-item__snippet').text if vacancy.select_one('.vacancy-serp-item__snippet') else None
        skills = [skill.text for skill in vacancy.select('.bloko-tag__section_text')] if vacancy.select('.bloko-tag__section_text') else []
        employment_format = vacancy.select_one('.vacancy-serp-item__meta-info').text if vacancy.select_one('.vacancy-serp-item__meta-info') else None
        salary_element = vacancy.select_one('.vacancy-serp-item__sidebar')

        salary = None
        if salary_element:
            salary_text = salary_element.text.replace('руб.', '').replace(' ', '')
            try:
                salary = int(salary_text)
            except ValueError:
                salary = None

        app_logger.info(f"Parsed vacancy: title={title}, description={description}, skills={skills}, employment_format={employment_format}, salary={salary}")

        if title and description:
            vac = Vacancy(
                title=title,
                description=description,
                skills=', '.join(skills),
                employment_format=employment_format,
                salary=salary
            )
            db.add(vac)
            db.commit()
            db.refresh(vac)
            vacancies.append(vac)
        else:
            app_logger.warning(f"Skipping vacancy with missing title or description: title={title}, description={description}")
    
    app_logger.info(f"Total vacancies fetched: {len(vacancies)}")
    return vacancies

@app.get("/applicants", response_model=List[ApplicantResponse])
def get_applicants(query: str, db: Session = Depends(get_db)):
    url = f'https://hh.ru/search/resume?text={query}'
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    app_logger.info(f"Fetching data from: {url}")
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        app_logger.error(f"Error fetching data from hh.ru: {response.status_code}")
        raise HTTPException(status_code=response.status_code, detail="Error fetching data from hh.ru")
    encoding = 'utf-8'
    decoded_content = response.content.decode(encoding)
    
    app_logger.info(f"Response status code: {response.status_code}")
    app_logger.info(f"Response content length: {len(response.text)}")
    app_logger.info(f"Response content preview: {response.content[:500]}")

    encoding = chardet.detect(response.content)['encoding']
    app_logger.info(f"Detected encoding: {encoding}")

    decoded_content = response.content.decode(encoding)
    soup = BeautifulSoup(decoded_content, 'html.parser')
    app_logger.info(f"Soup object created: {soup.prettify()[:500]}")

    applicants = []
    for applicant in soup.select('.resume-serp-item'):
        name = applicant.select_one('.resume-search-item__fullname').text if applicant.select_one('.resume-search-item__fullname') else None
        skills = [skill.text for skill in applicant.select('.bloko-tag__section_text')] if applicant.select('.bloko-tag__section_text') else []

        app_logger.info(f"Parsed applicant: name={name}, skills={skills}")

        if name:
            appl = Applicant(
                name=name,
                skills=', '.join(skills)
            )
            db.add(appl)
            db.commit()
            db.refresh(appl)
            applicants.append(appl)
        else:
            app_logger.warning(f"Skipping applicant with missing name: name={name}")
    
    app_logger.info(f"Total applicants fetched: {len(applicants)}")
    return applicants

@app.get("/analytics/vacancies")
def get_vacancies_analytics(db: Session = Depends(get_db)):
    num_vacancies = db.query(Vacancy).count()
    return {"num_vacancies": num_vacancies}

@app.get("/analytics/applicants")
def get_applicants_analytics(db: Session = Depends(get_db)):
    num_applicants = db.query(Applicant).count()
    return {"num_applicants": num_applicants}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)