#[derive(Debug, Clone, PartialEq)]
pub enum DataValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Int,
    Float,
    Bool,
    Str,
}

#[derive(Debug, Clone)]
pub struct DataFrame {
    columns: Vec<String>,
    column_types: Vec<ColumnType>,
    data: Vec<Vec<DataValue>>,
}

impl DataFrame {
    pub fn new(columns: Vec<String>, column_types: Vec<ColumnType>) -> Self {
        DataFrame {
            columns,
            column_types,
            data: Vec::new(),
        }
    }
    
    pub fn from_rows(columns: Vec<String>, column_types: Vec<ColumnType>, rows: Vec<Vec<DataValue>>) -> Self {
        DataFrame { columns, column_types, data: rows }
    }

    pub fn read_csv(file_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut rdr = csv::Reader::from_path(file_path)?;
        let headers: Vec<String> = rdr.headers()?.iter().map(String::from).collect();
        let raw_rows = Self::collect_raw_rows(&mut rdr)?;
        let column_types = Self::infer_column_types(&headers, &raw_rows);
        let rows = raw_rows
            .iter()
            .map(|raw_row| Self::parse_row(raw_row, &column_types))
            .collect();

        Ok(DataFrame::from_rows(headers, column_types, rows))
    }

    fn collect_raw_rows<R: std::io::Read>(
        rdr: &mut csv::Reader<R>,
    ) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
        let mut raw_rows = Vec::new();
        for result in rdr.records() {
            let record = result?;
            raw_rows.push(record.iter().map(|s| s.to_string()).collect());
        }
        Ok(raw_rows)
    }

    fn infer_column_types(headers: &[String], raw_rows: &[Vec<String>]) -> Vec<ColumnType> {
        (0..headers.len())
            .map(|col_idx| {
                let mut is_int = true;
                let mut is_float = true;
                let mut is_bool = true;
                for row in raw_rows {
                    let val = row.get(col_idx).map(|s| s.trim()).unwrap_or("");
                    if val.is_empty() {
                        continue;
                    }
                    if is_int && val.parse::<i64>().is_err() {
                        is_int = false;
                    }
                    if is_float && val.parse::<f64>().is_err() {
                        is_float = false;
                    }
                    if is_bool && val != "true" && val != "false" {
                        is_bool = false;
                    }
                }
                if is_int {
                    ColumnType::Int
                } else if is_float {
                    ColumnType::Float
                } else if is_bool {
                    ColumnType::Bool
                } else {
                    ColumnType::Str
                }
            })
            .collect()
    }

    fn parse_row(raw_row: &Vec<String>, column_types: &[ColumnType]) -> Vec<DataValue> {
        raw_row
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let s = s.trim();
                if s.is_empty() {
                    DataValue::Null
                } else {
                    match column_types[i] {
                        ColumnType::Int => s.parse::<i64>().map(DataValue::Int).unwrap_or(DataValue::Null),
                        ColumnType::Float => s.parse::<f64>().map(DataValue::Float).unwrap_or(DataValue::Null),
                        ColumnType::Bool => match s {
                            "true" => DataValue::Bool(true),
                            "false" => DataValue::Bool(false),
                            _ => DataValue::Null,
                        },
                        ColumnType::Str => DataValue::Str(s.to_string()),
                    }
                }
            })
            .collect()
    }
    
    pub fn write_csv(&self, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut wtr = csv::Writer::from_path(file_path)?;
        wtr.write_record(&self.columns)?;

        for row in &self.data {
            let record: Vec<String> = row.iter().map(|v| match v {
                DataValue::Int(i) => i.to_string(),
                DataValue::Float(f) => f.to_string(),
                DataValue::Bool(b) => b.to_string(),
                DataValue::Str(s) => s.clone(),
                DataValue::Null => String::new(),
            }).collect();
            wtr.write_record(record)?;
        }

        wtr.flush()?;
        Ok(())
    }

    pub fn head(&self, n: usize) -> DataFrame {
        let mut head_rows = Vec::new();
        for row in self.data.iter().take(n) {
            head_rows.push(row.clone());
        }
        DataFrame::from_rows(self.columns.clone(), self.column_types.clone(), head_rows)
    }

    pub fn tail(&self, n: usize) -> DataFrame {
        let mut tail_rows = Vec::new();
        for row in self.data.iter().rev().take(n) {
            tail_rows.push(row.clone());
        }
        tail_rows.reverse(); // Reverse to maintain original order
        DataFrame::from_rows(self.columns.clone(), self.column_types.clone(), tail_rows)
    }

    pub fn shape(&self) -> (usize, usize) {
        (self.data.len(), self.columns.len())
    }

    pub fn info(&self) -> String {
        let mut info = format!(
            "DataFrame with {} rows and {} columns\n",
            self.data.len(),
            self.columns.len()
        );
        info.push_str("Columns:\n");
        for (i, col) in self.columns.iter().enumerate() {
            let dtype = match self.column_types.get(i) {
                Some(ColumnType::Int) => "Integer",
                Some(ColumnType::Float) => "Float",
                Some(ColumnType::Bool) => "Boolean",
                Some(ColumnType::Str) => "String",
                None => "Unknown",
            };
            info.push_str(&format!("{}: {} ({})\n", i, col, dtype));
        }
        print!("{}", info);
        info
    }

    pub fn describe(&self) -> String {
        use std::f64;

        let mut desc = String::new();

        for (i, col) in self.columns.iter().enumerate() {
            match self.column_types.get(i) {
                Some(ColumnType::Int) | Some(ColumnType::Float) => {
                    // Collect all numeric values in this column
                    let nums: Vec<f64> = self.data.iter()
                        .filter_map(|row| match row.get(i) {
                            Some(DataValue::Int(v)) => Some(*v as f64),
                            Some(DataValue::Float(v)) => Some(*v),
                            _ => None,
                        })
                        .collect();

                    if nums.is_empty() {
                        continue; // Skip non-numeric columns
                    }

                    let count = nums.len();
                    let mean = nums.iter().sum::<f64>() / count as f64;
                    let min = nums.iter().cloned().fold(f64::INFINITY, f64::min);
                    let max = nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                    let std = if count > 1 {
                        let var = nums.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (count as f64);
                        var.sqrt()
                    } else {
                        0.0
                    };

                    desc.push_str(&format!(
                        "Column: {}\nCount: {}\nMean: {:.2}\nStd: {:.2}\nMin: {:.2}\nMax: {:.2}\n\n",
                        col, count, mean, std, min, max
                    ));
                }
                _ => {}
            }
        }

        print!("{}", desc);
        desc
    }

    pub fn filter<F>(&self, column: &str, filter_func: F) -> DataFrame
    where
        F: Fn(&DataValue) -> bool,
    {
        let col_idx = match self.columns.iter().position(|c| c == column) {
            Some(idx) => idx,
            None => return DataFrame::from_rows(self.columns.clone(), self.column_types.clone(), vec![]), // Return empty if column not found
        };

        let filtered_rows: Vec<Vec<DataValue>> = self
            .data
            .iter()
            .filter(|row| {
                row.get(col_idx)
                    .map(|val| filter_func(val))
                    .unwrap_or(false)
            })
            .cloned()
            .collect();

        DataFrame::from_rows(self.columns.clone(), self.column_types.clone(), filtered_rows)
    }

    pub fn sort<F>(&self, column: &str, sort_func: F) -> DataFrame
    where
        F: Fn(&DataValue, &DataValue) -> std::cmp::Ordering,
    {
        let col_idx = match self.columns.iter().position(|c| c == column) {
            Some(idx) => idx,
            None => return DataFrame::from_rows(self.columns.clone(), self.column_types.clone(), vec![]), // Return empty if column not found
        };

        let mut sorted_rows = self.data.clone();
        sorted_rows.sort_by(|a, b| {
            let va = a.get(col_idx).unwrap_or(&DataValue::Null);
            let vb = b.get(col_idx).unwrap_or(&DataValue::Null);
            sort_func(va, vb)
        });

        DataFrame::from_rows(self.columns.clone(), self.column_types.clone(), sorted_rows)
    }

    pub fn apply(&self, column: &str, func: &dyn Fn(&DataValue) -> DataValue) -> DataFrame {
        let col_idx = match self.columns.iter().position(|c| c == column) {
            Some(idx) => idx,
            None => return DataFrame::from_rows(self.columns.clone(), self.column_types.clone(), vec![]), // Return empty if column not found
        };

        let mut new_data = self.data.clone();
        for row in &mut new_data {
            if let Some(value) = row.get_mut(col_idx) {
                *value = func(value);
            }
        }

        DataFrame::from_rows(self.columns.clone(), self.column_types.clone(), new_data)
    }

    pub fn get_value(&self, row: usize, col: &str) -> Option<&DataValue> {
        let col_idx = self.columns.iter().position(|c| c == col)?;
        self.data.get(row).and_then(|r| r.get(col_idx))
    }

    pub fn get_values(&self, col: &str) -> Option<Vec<&DataValue>> {
        let col_idx = self.columns.iter().position(|c| c == col)?;
        Some(self.data.iter().filter_map(|r| r.get(col_idx)).collect())
    }

    pub fn get_unique_values(&self, col: &str) -> Option<Vec<DataValue>> {
        let col_idx = self.columns.iter().position(|c| c == col)?;
        let mut unique_values = Vec::new();
        for row in &self.data {
            if let Some(value) = row.get(col_idx) {
                if !unique_values.contains(value) {
                    unique_values.push(value.clone());
                }
            }
        }
        Some(unique_values)
    }
}
