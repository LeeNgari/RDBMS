package engine

func BuildIndexes(table *Table) error {
	for _, col := range table.Schema.Columns {

		// Only build indexes for primary or unique columns
		if !col.PrimaryKey && !col.Unique {
			continue
		}

		idx := &Index{
			Column: col.Name,
			Data:   make(map[interface{}][]int),
			Unique: col.PrimaryKey || col.Unique,
		}

		for rowPos, row := range table.Rows {
			val, ok := row[col.Name]
			if !ok {
				continue 
			}

			idx.Data[val] = append(idx.Data[val], rowPos)

			if idx.Unique && len(idx.Data[val]) > 1 {
				return &ConstraintError{
					Table:  table.Name,
					Column: col.Name,
					Value:  val,
				}
			}
		}

		table.Indexes[col.Name] = idx
	}

	return nil
}
func BuildDatabaseIndexes(db *Database) error {
	for _, table := range db.Tables {
		if err := BuildIndexes(table); err != nil {
			return err
		}
	}
	return nil
}
