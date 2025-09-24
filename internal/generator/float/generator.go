package float

// func Accept(
// 	_ context.Context,
// 	optUserSettings mo.Option[config.Generator],
// 	optBaseType mo.Option[model.TargetType],
// ) (generator.AcceptanceDecision, error) {
// 	userSettings, userPresented := optUserSettings.Get()
// 	if userPresented && userSettings.Type != config.GeneratorTypeFloat {
// 		return generator.AcceptanceDecision{}, fmt.Errorf("%w: accept", generator.ErrGeneratorDeclined)
// 	}
// 	baseType, baseTypePresented := optBaseType.Get()
// 	if !userPresented && baseTypePresented && baseType.Type != model.Float {
// 		return generator.AcceptanceDecision{}, fmt.Errorf("%w: accept", generator.ErrGeneratorDeclined)
// 	}

// 	return generator.AcceptanceDecision{}, nil
// }
