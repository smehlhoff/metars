// #![warn(clippy::all)]
// #![warn(clippy::nursery)]
// #![warn(clippy::pedantic)]

use std::fs::{self, File};
use std::io::{BufReader, BufWriter};

use std::io;

use chrono::Utc;
use flate2::read::GzDecoder;
use polars::frame::DataFrame;
use polars::io::SerReader;
use polars::prelude::CsvReadOptions;

#[derive(Debug)]
enum Temperature {
    Celsius(Option<f64>),
    Fahrenheit(Option<f64>),
}

impl Temperature {
    fn to_fahrenheit(&self) -> Option<f64> {
        match *self {
            Self::Celsius(Some(val)) => Some(val.mul_add(1.8, 32.0)),
            Self::Fahrenheit(Some(val)) => Some(val),
            _ => None,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
enum WindDirection {
    Degrees(Option<i32>),
    Variable(Option<String>),
}

impl WindDirection {
    fn to_cardinal_direction(&self) -> Option<String> {
        match *self {
            Self::Degrees(Some(val)) => {
                if val == 0 {
                    None
                } else {
                    let directions: [&str; 17] = [
                        "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW",
                        "W", "WNW", "NW", "NNW", "N",
                    ];
                    let index = (f64::from(val) / 22.5).round();
                    let direction = directions[index as usize];

                    Some(String::from(direction))
                }
            }
            Self::Variable(_) => Some(String::from("Variable")),
            _ => None,
        }
    }
}

#[derive(Debug)]
enum Wind {
    Knots(Option<f64>),
    Mph(Option<f64>),
}

impl Wind {
    fn to_mph(&self) -> Option<f64> {
        match *self {
            Self::Knots(Some(val)) => {
                let result = val * 1.15078;
                Some((result * 100.00).floor() / 100.0)
            }
            Self::Mph(Some(val)) => Some(val),
            _ => None,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct Cloud {
    sky_cover: Option<String>,
    sky_cover_label: Option<String>,
    cloud_base_ft_agl: Option<i32>,
}

impl Cloud {
    fn sky_cover_label(&mut self) {
        let sky_cover_label = match &self.sky_cover {
            Some(val) => match val.as_ref() {
                "CLR" | "SKC" => Some(String::from("Clear")),
                "FEW" => Some(String::from("Few")),
                "SCT" => Some(String::from("Scattered")),
                "BKN" => Some(String::from("Broken")),
                "OVC" => Some(String::from("Overcast")),
                "OVX" => Some(String::from("Obscured")),
                _ => Some(String::new()),
            },
            None => None,
        };

        self.sky_cover_label = sky_cover_label;
    }
}

#[allow(dead_code)]
#[derive(Debug)]
enum Elevation {
    Meters(Option<f64>),
    Feet(Option<f64>),
}

impl Elevation {
    fn to_feet(&self) -> Option<f64> {
        match *self {
            Self::Meters(Some(val)) => Some((val * 3.28084).round()),
            Self::Feet(Some(val)) => Some(val),
            _ => None,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct Metar {
    raw_text: String,
    station_id: String,
    observation_time: Option<chrono::DateTime<Utc>>,
    lat: Option<f64>,
    lon: Option<f64>,
    temp_c: Temperature,
    temp_f: Temperature,
    dewpoint_c: Temperature,
    dewpoint_f: Temperature,
    wind_dir_degrees: WindDirection,
    wind_dir_cardinal: Option<String>,
    wind_speed_kt: Wind,
    wind_speed_mph: Wind,
    wind_gust_kt: Wind,
    wind_gust_mph: Wind,
    visibility_statute_mi: Option<f64>,
    clouds: Vec<Cloud>,
    altim_in_hg: Option<f64>,
    wx_string: Option<String>,
    flight_category: Option<String>,
    report_type: Option<String>,
    elevation_m: Elevation,
    elevation_ft: Elevation,
    remarks: Option<String>,
}

#[derive(Debug)]
struct Metars {
    conus: Vec<Metar>,
}

impl Metar {
    async fn fetch_metars() -> Result<(), Box<dyn std::error::Error>> {
        let url = "https://aviationweather.gov/data/cache/metars.cache.csv.gz";
        let resp = reqwest::get(url).await?;

        if resp.status() != 200 {
            return Err(format!("Failed to download file: HTTP {}", resp.status()).into());
        }

        let file = File::create("./metars.gz")?;
        let mut writer = BufWriter::new(file);

        io::copy(&mut resp.bytes().await?.as_ref(), &mut writer)?;

        Ok(())
    }

    fn extract_metar_file(path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let gz = File::open(path)?;
        let decompressed = GzDecoder::new(gz);
        let out = File::create("./metars.csv")?;
        let mut writer = BufWriter::new(out);

        io::copy(&mut BufReader::new(decompressed), &mut writer)?;

        fs::remove_file(path)?;

        Ok(())
    }

    fn read_metar_file(path: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(path)?;

        let lines: Vec<&str> = contents.split('\n').collect();

        if lines[0].contains("No errors") {
            let lines = &lines[5..];

            let data = lines.join("\n");
            let data = data.strip_suffix("\n").unwrap_or(&data);

            fs::write(path, data)?;
        }

        let dataframe = CsvReadOptions::default()
            .with_infer_schema_length(None)
            .try_into_reader_with_file_path(Some(path.into()))?
            .finish()?;

        fs::remove_file(path)?;

        Ok(dataframe)
    }

    fn parse_metars(dataframe: &DataFrame) -> Metars {
        let mut metars: Vec<Self> = Vec::new();

        for i in 0..dataframe.height() {
            if let Some(row) = dataframe.get(i) {
                let station_id = row[1].str_value().to_string();

                if station_id.starts_with('K') {
                    let raw_text = row[0].str_value().to_string();

                    let observation_time: Option<chrono::DateTime<Utc>> = if row[2].is_null() {
                        None
                    } else {
                        match row[2].str_value().to_string().parse() {
                            Ok(val) => Some(val),
                            Err(_) => None,
                        }
                    };

                    let lat = row[3].str_value().parse::<f64>().ok();
                    let lon = row[4].str_value().parse::<f64>().ok();

                    let temp_c = if row[5].is_null() {
                        Temperature::Celsius(None)
                    } else {
                        match row[5].str_value().parse::<f64>() {
                            Ok(val) => Temperature::Celsius(Some(val)),
                            Err(_) => Temperature::Celsius(None),
                        }
                    };

                    let temp_f = Temperature::Fahrenheit(temp_c.to_fahrenheit());

                    let dewpoint_c = if row[6].is_null() {
                        Temperature::Celsius(None)
                    } else {
                        match row[6].str_value().parse::<f64>() {
                            Ok(val) => Temperature::Celsius(Some(val)),
                            Err(_) => Temperature::Celsius(None),
                        }
                    };

                    let dewpoint_f = Temperature::Fahrenheit(dewpoint_c.to_fahrenheit());

                    let wind_dir_degrees = if row[7].is_null() {
                        WindDirection::Degrees(None)
                    } else if row[7].str_value() == "VRB" {
                        WindDirection::Variable(Some(String::from("VRB")))
                    } else {
                        match row[7].str_value().parse::<i32>() {
                            Ok(val) => WindDirection::Degrees(Some(val)),
                            Err(_) => WindDirection::Degrees(None),
                        }
                    };

                    let wind_dir_cardinal = wind_dir_degrees.to_cardinal_direction();

                    let wind_speed_kt = if row[8].is_null() {
                        Wind::Knots(None)
                    } else {
                        match row[8].str_value().parse::<f64>() {
                            Ok(val) => Wind::Knots(Some(val)),
                            Err(_) => Wind::Knots(None),
                        }
                    };

                    let wind_speed_mph = Wind::Mph(wind_speed_kt.to_mph());

                    let wind_gust_kt = if row[9].is_null() {
                        Wind::Knots(None)
                    } else {
                        match row[9].str_value().parse::<f64>() {
                            Ok(val) => Wind::Knots(Some(val)),
                            Err(_) => Wind::Knots(None),
                        }
                    };

                    let wind_gust_mph = Wind::Mph(wind_gust_kt.to_mph());

                    let visibility_statute_mi = if row[10].is_null() {
                        None
                    } else {
                        let val = row[10].str_value().replace('+', "");

                        match val.parse::<f64>() {
                            Ok(val) => Some(val),
                            Err(_) => None,
                        }
                    };

                    let altim_in_hg = if row[11].is_null() {
                        None
                    } else {
                        match row[11].str_value().parse::<f64>() {
                            Ok(val) => Some(val),
                            Err(_) => None,
                        }
                    };

                    let mut clouds = Vec::new();

                    for i in (22..=28).step_by(2) {
                        let sky_cover = if row[i].is_null() {
                            None
                        } else {
                            Some(row[i].str_value().to_string())
                        };

                        let cloud_base = if row[i + 1].is_null() {
                            None
                        } else {
                            match row[i + 1].str_value().parse::<i32>() {
                                Ok(val) => Some(val),
                                Err(_) => None,
                            }
                        };

                        if sky_cover.is_none() && cloud_base.is_none() {
                            continue;
                        };

                        let mut cloud = Cloud {
                            sky_cover,
                            cloud_base_ft_agl: cloud_base,
                            sky_cover_label: None,
                        };

                        cloud.sky_cover_label();

                        clouds.push(cloud);
                    }

                    let wx_string = if row[21].is_null() {
                        None
                    } else {
                        Some(row[21].str_value().to_string())
                    };

                    let flight_category = if row[30].is_null() {
                        None
                    } else {
                        Some(row[30].str_value().to_string())
                    };

                    let report_type = if row[42].is_null() {
                        None
                    } else {
                        Some(row[42].str_value().to_string())
                    };

                    let elevation_m = if row[43].is_null() {
                        Elevation::Meters(None)
                    } else {
                        match row[43].str_value().parse::<f64>() {
                            Ok(val) => {
                                if val == 9999.0 {
                                    Elevation::Meters(None)
                                } else {
                                    Elevation::Meters(Some(val))
                                }
                            }
                            Err(_) => Elevation::Meters(None),
                        }
                    };

                    let elevation_ft = Elevation::Feet(elevation_m.to_feet());

                    let remarks = if row[0].is_null() {
                        None
                    } else {
                        let remarks = row[0].str_value();

                        if remarks.contains("RMK") {
                            let remarks: Vec<&str> = remarks.split(' ').collect();
                            let index = remarks.iter().position(|&x| x == "RMK");

                            match index {
                                Some(val) => Some(remarks[val + 1..].join(" ")),
                                None => None,
                            }
                        } else {
                            None
                        }
                    };

                    let metar = Self {
                        raw_text,
                        station_id,
                        observation_time,
                        lat,
                        lon,
                        temp_c,
                        temp_f,
                        dewpoint_c,
                        dewpoint_f,
                        wind_dir_degrees,
                        wind_dir_cardinal,
                        wind_speed_kt,
                        wind_speed_mph,
                        wind_gust_kt,
                        wind_gust_mph,
                        visibility_statute_mi,
                        clouds,
                        altim_in_hg,
                        wx_string,
                        flight_category,
                        report_type,
                        elevation_m,
                        elevation_ft,
                        remarks,
                    };

                    metars.push(metar);
                }
            }
        }

        Metars { conus: metars }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Metar::fetch_metars().await?;
    Metar::extract_metar_file("./metars.gz")?;

    let dataframe = Metar::read_metar_file("./metars.csv")?;
    let metars = Metar::parse_metars(&dataframe);

    for metar in metars.conus {
        if metar.station_id == "KSJC" {
            println!("{:?}", metar)
        }
    }

    Ok(())
}
