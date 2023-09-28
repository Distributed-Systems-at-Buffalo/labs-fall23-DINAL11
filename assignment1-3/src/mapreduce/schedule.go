package mapreduce

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO

	// Initialize  number of tasks
	x := 0
	// Create a channel to receive task completion signals.
	taskcomchannel := make(chan bool, ntasks)

	for i := 0; i < ntasks; i++ {
		go func(numtask int) {
			for {
				// Wait for a registered worker.
				regworker := <-mr.registerChannel

				//DoTaskArgs for task.
				args := DoTaskArgs{
					JobName:       mr.jobName,
					File:          mr.files[numtask],
					Phase:         phase,
					TaskNumber:    numtask,
					NumOtherPhase: nios,
				}

				// initiates a task on worker using an RPC
				rpccall := call(regworker, "Worker.DoTask", &args, new(struct{}))
				if rpccall {
					// notify the channel of task completion.
					taskcomchannel <- true

					// return the worker by sending it to the register channel.
					go func() {
						mr.registerChannel <- regworker
					}()
					break
				}
			}
		}(i)
	}

	// Wait for all tasks to complete.
	for x < ntasks {
		<-taskcomchannel
		x++
	}

	close(taskcomchannel)

	debug("Schedule: %v phase done\n", phase)
}
