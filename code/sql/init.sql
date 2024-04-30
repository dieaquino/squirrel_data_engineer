
CREATE TABLE IF NOT EXISTS Areas (
    AreaID VARCHAR(5) PRIMARY KEY NOT NULL,
    AreaName VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Parks (
    ParkID INT PRIMARY KEY NOT NULL,
    AreaID VARCHAR(50),
    ParkName VARCHAR(50),
    FOREIGN KEY (AreaID) REFERENCES Areas(AreaID)
);

CREATE TABLE IF NOT EXISTS ParkSections (
    ParkSectionID INT PRIMARY KEY NOT NULL,
    ParkID INT,
    ParkSectionName VARCHAR(50),
    FOREIGN KEY (ParkID) REFERENCES Parks(ParkID)
);

CREATE TABLE IF NOT EXISTS ParkLitters (
    ParkLitterID INT PRIMARY KEY NOT NULL,
    ParkLitterName VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Animals (
    AnimalID INT PRIMARY KEY NOT NULL,
    AnimalName VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS ParkConditions (
    ParkConditionID INT PRIMARY KEY NOT NULL,
    ParkConditionName VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS ParkStatus (
    ParkStatusID INT PRIMARY KEY NOT NULL,
    ParkID INT,
    SectionID INT,
    ParkStatusDate DATE NOT NULL,
    ParkStatusStartTime TIME NOT NULL,
    ParkStatusEndTime TIME NOT NULL,
    TotalTime INT,
    ParkConditionID INT,
    ParkLitterID INT,
    ParkConditionObs VARCHAR(250),
    ParkLitterObs VARCHAR(250),
    ParkTemperatureDegrees NUMERIC(3,2),
    FOREIGN KEY (ParkID) REFERENCES Parks(ParkID),
    FOREIGN KEY (SectionID) REFERENCES ParkSections(ParkSectionID),
    FOREIGN KEY (ParkConditionID) REFERENCES ParkConditions(ParkConditionID),
    FOREIGN KEY (ParkLitterID) REFERENCES ParkLitters(ParkLitterID)
);

CREATE TABLE IF NOT EXISTS Weather (
    WeatherID INT PRIMARY KEY NOT NULL,
    WeatherName VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS ParkStatusWeather (
    ParkStatusWeatherID INT PRIMARY KEY NOT NULL,
    ParkStatusID INT,
    WeatherID INT,
    ParkStatusWeatherObs VARCHAR(250),
    FOREIGN KEY (ParkStatusID) REFERENCES ParkStatus(ParkStatusID),
    FOREIGN KEY (WeatherID) REFERENCES Weather(WeatherID)
);


CREATE TABLE IF NOT EXISTS Colors (
    ColorID INT PRIMARY KEY NOT NULL,
    ColorName VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS Squirrels (
    SquirrelID VARCHAR(15) PRIMARY KEY NOT NULL,
    ParkID INT,
    SquirrelPrimFurColor INT REFERENCES Colors(ColorID),
    SquirrelHighFurColor INT REFERENCES Colors(ColorID),
    SquirrelColorNotes VARCHAR(250),
    SquirrelSpecificLocation VARCHAR(250),
    SquirrelObservations VARCHAR(250),
    SquirrelLatitude VARCHAR(40),
    SquirrelLongitude VARCHAR(40),
    SquirrelMinHeightFeet NUMERIC(2,2),
    SquirrelMaxHeightFeet NUMERIC(2,2),
    FOREIGN KEY (ParkID) REFERENCES Parks(ParkID)
);

CREATE TABLE IF NOT EXISTS Locations (
    LocationID INT PRIMARY KEY NOT NULL,
    LocationName VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Activities (
    ActivityID INT PRIMARY KEY NOT NULL,
    ActivityName VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SquirrelActivities (
    SquirrelActivityID INT PRIMARY KEY NOT NULL,
    SquirrelID VARCHAR(15),
    ActivityID INT,
    FOREIGN KEY (SquirrelID) REFERENCES Squirrels(SquirrelID),
    FOREIGN KEY (ActivityID) REFERENCES Activities(ActivityID)
);

CREATE TABLE IF NOT EXISTS Interactions (
    InteractionID INT PRIMARY KEY NOT NULL,
    InteractionName VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS SquirrelHumanInteractions (
    SquirrelHumanInteractionID INT PRIMARY KEY NOT NULL,
    SquirrelID VARCHAR(15),
    InteractionID INT,
    FOREIGN KEY (SquirrelID) REFERENCES Squirrels(SquirrelID),
    FOREIGN KEY (InteractionID) REFERENCES Interactions(InteractionID)
);

CREATE TABLE IF NOT EXISTS ParkStatusAnimals (
    ParkStatusAnimalID INT PRIMARY KEY NOT NULL,
    ParkStatusID INT,
    AnimalID INT,
    Observations VARCHAR(250),
    FOREIGN KEY (ParkStatusID) REFERENCES ParkStatus(ParkStatusID),
    FOREIGN KEY (AnimalID) REFERENCES Animals(AnimalID)
);

CREATE TABLE IF NOT EXISTS SquirrelLocations (
    SquirrelLocationID INT PRIMARY KEY NOT NULL,
    SquirrelID VARCHAR(15),
    LocationID INT,
    FOREIGN KEY (SquirrelID) REFERENCES Squirrels(SquirrelID),
    FOREIGN KEY (LocationID) REFERENCES Locations(LocationID)
);
