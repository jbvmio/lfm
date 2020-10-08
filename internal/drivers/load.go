package drivers

import (
	"fmt"
	"sort"

	"github.com/jbvmio/lfm"
	"github.com/jbvmio/lfm/driver"
	"github.com/jbvmio/lfm/driver/config"
)

// LoadProcessors loads processing Drivers.
func LoadProcessors(cfg lfm.Configs) (processors map[string][][][]driver.Driver, err error) {
	processors = make(map[string][][][]driver.Driver)
	for k, v := range cfg {
		var stageOrder []int
		for _, s := range v.Processors {
			stageOrder = append(stageOrder, s.Stage)
		}

		fmt.Println("Total Stages:", len(stageOrder))

		sort.SliceStable(stageOrder, func(i, j int) bool {
			return stageOrder[i] < stageOrder[j]
		})
		if checkNumDupes(stageOrder) {
			return nil, fmt.Errorf("%s has duplicate stage number defined", k)
		}
		stages := make([][][]driver.Driver, len(stageOrder))
		for STAGE, o := range stageOrder {
			for _, processor := range v.Processors {
				if processor.Stage == o {
					var stepOrder []int
					for _, st := range processor.Steps {
						stepOrder = append(stepOrder, st.Step)
					}
					sort.SliceStable(stepOrder, func(i, j int) bool {
						return stepOrder[i] < stepOrder[j]
					})

					fmt.Println("Total Steps for Stage", o, ">", len(stepOrder), ">", stepOrder)

					if checkNumDupes(stepOrder) {
						return nil, fmt.Errorf("%s stage %d has missing or duplicate step numbers", k, o)
					}
					stages[STAGE] = make([][]driver.Driver, len(stepOrder))

					//tags := driver.NewKVStore()
					//vars := driver.NewKVStore()

					for STEP, sto := range stepOrder {
						for _, step := range processor.Steps {
							if step.Step == sto {
								var drivers []driver.Driver

								d, err := config.FromConfig(step.Workflow)
								if err != nil {
									return nil, fmt.Errorf("invalid configuration for %s stage %d step %d: %v", k, o, sto, err)
								}
								//d.UseKV(driver.TagsLabel, tags)
								//d.UseKV(driver.VarsLabel, vars)
								drivers = append(drivers, d)

								/*
									for _, wf := range step.Workflow {
										d, err := driver.FromConfig(wf)
										if err != nil {
											return nil, fmt.Errorf("invalid configuration for %s stage %d step %d: %v", k, o, sto, err)
										}
										drivers = append(drivers, d)
									}
								*/

								fmt.Println("Total Drivers for Stage", o, "> Step", sto, ">", len(drivers))

								stages[STAGE][STEP] = drivers
							}

						}
					}
					processors[k] = stages
				}
			}
		}
	}
	return processors, nil
}

func checkNumDupes(n []int) (hasDupe bool) {
	dupe := make(map[int]bool)
	for _, x := range n {
		if !dupe[x] {
			dupe[x] = true
			continue
		}
		hasDupe = true
		return
	}
	return
}
