-- Create places table
CREATE TABLE IF NOT EXISTS places (
    city VARCHAR(255) NOT NULL,
    county VARCHAR(255) NOT NULL,
    country VARCHAR(255) NOT NULL
);

-- Create people table
CREATE TABLE IF NOT EXISTS people (
    given_name VARCHAR(255) NOT NULL,
    family_name VARCHAR(255) NOT NULL,
    date_of_birth DATE NOT NULL,
    place_of_birth VARCHAR(255) NOT NULL,
    FOREIGN KEY (place_of_birth) REFERENCES places(city)
);

-- Create index on city for faster querying
CREATE INDEX idx_city ON places(city);