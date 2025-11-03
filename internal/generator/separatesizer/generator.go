package separatesizer

import "github.com/jmozgit/datagen/internal/model"

type Generator struct {
	model.Generator
	tableName model.TableName
	column    model.Identifier
}

func NewGenerator(
	gen model.Generator,
	tableName model.TableName,
	column model.Identifier,
) model.Generator {
	return &Generator{
		Generator: gen,
		tableName: tableName,
		column:    column,
	}
}

func (g *Generator) GenerateFor() (model.TableName, model.Identifier) {
	return g.tableName, g.column
}
