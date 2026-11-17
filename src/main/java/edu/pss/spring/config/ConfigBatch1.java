package edu.pss.spring.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class ConfigBatch1 {

	@Value("${batch.ruta.entrada}")
	private String rutaEntrada;

	@Value("${batch.fichero}")
	private String nombreFichero;

	private int totalPasos;

	@Bean
	public StepExecutionListener listenerStep() {
		return new StepExecutionListener() {

			@Override
			public void beforeStep(StepExecution stepExecution) {
				ExecutionContext jobContexto = stepExecution.getJobExecution().getExecutionContext();

				int stepsIniciados = jobContexto.getInt("stepsIniciados", 0) + 1;
				jobContexto.put("stepsIniciados", stepsIniciados);

				// DATO 4
				jobContexto.put("porcentajePasos", stepsIniciados * 100.0 / totalPasos);
			}
		};
	}

	@Bean
	public JobExecutionListener listenerJob() {
		return new JobExecutionListener() {

			@Override
			public void afterJob(JobExecution jobExecution) {
				ExecutionContext jobContexto = jobExecution.getExecutionContext();

				System.out.println("1) Localidades distintas: " + jobContexto.getInt("numLocalidades", 0));
				System.out.println("2) Localidad con más clientes: " + jobContexto.getString("localidadMasClientes", ""));
				System.out.println("3) Localidad nombre más largo: " + jobContexto.getString("localidadMasLarga", ""));
				System.out.println("4) Porcentaje de pasos iniciados: " + jobContexto.getDouble("porcentajePasos", 0.0) + " %");
			}
		};
	}

	@Bean
	public Step comprobarFichero(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			StepExecutionListener listenerStep) {
		return new StepBuilder("comprobarFichero", jobRepository)
				.tasklet((StepContribution contribution, ChunkContext chunkContext) -> {
					Path fichero = Paths.get(this.rutaEntrada, this.nombreFichero);
					if (!Files.exists(fichero)) {
						throw new RuntimeException("No existe el fichero " + fichero);
					}
					return RepeatStatus.FINISHED;
				}, transactionManager).listener(listenerStep).build();
	}

	@Bean
	public Step crearDirectorioSalida(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			StepExecutionListener listenerStep) {
		return new StepBuilder("crearDirectorioSalida", jobRepository)
				.tasklet((StepContribution contribution, ChunkContext chunkContext) -> {
					return RepeatStatus.FINISHED;
				}, transactionManager).listener(listenerStep).build();
	}

	@Bean
	public Step procesarFichero(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			StepExecutionListener listenerStep) {
		return new StepBuilder("procesarFichero", jobRepository)
				.tasklet((StepContribution contribution, ChunkContext chunkContext) -> {

					List<String> lineas = Files.readAllLines(Paths.get(this.rutaEntrada, this.nombreFichero));

					// ciudad de cada cliente correcto (con repetidas)
					List<String> ciudades = new ArrayList<>();

					for (int indice = 1; indice < lineas.size(); indice++) {
						String[] campos = lineas.get(indice).split(";");
						if (campos.length == 4) {
							ciudades.add(campos[3].trim().toUpperCase());
						}
					}

					// DATO 1: localidades distintas
					List<String> distintas = new ArrayList<>();
					for (String ciudad : ciudades) {
						if (!distintas.contains(ciudad)) {
							distintas.add(ciudad);
						}
					}

					// DATO 2 y DATO 3
					String masClientes = "";
					int maxClientes = 0;
					String masLarga = "";

					for (String localidad : distintas) {
						int clientes = 0;
						for (String ciudad : ciudades) {
							if (ciudad.equals(localidad)) {
								clientes++;
							}
						}
						if (clientes > maxClientes) {
							maxClientes = clientes;
							masClientes = localidad;
						}
						if (localidad.length() > masLarga.length()) {
							masLarga = localidad;
						}
					}

					ExecutionContext jobContexto = contribution.getStepExecution().getJobExecution().getExecutionContext();
					jobContexto.put("numLocalidades", distintas.size());
					jobContexto.put("localidadMasClientes", masClientes);
					jobContexto.put("localidadMasLarga", masLarga);

					return RepeatStatus.FINISHED;
				}, transactionManager).listener(listenerStep).build();
	}

	@Bean
	public Step archivarFichero(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			StepExecutionListener listenerStep) {
		return new StepBuilder("archivarFichero", jobRepository)
				.tasklet((StepContribution contribution, ChunkContext chunkContext) -> {
					return RepeatStatus.FINISHED;
				}, transactionManager).listener(listenerStep).build();
	}

	@Bean
	public Job jobGestionFichero(JobRepository jobRepository, Step comprobarFichero, Step crearDirectorioSalida,
			Step procesarFichero, Step archivarFichero, JobExecutionListener listenerJob) {

		Job trabajoCompleto = new JobBuilder("jobGestionFichero", jobRepository)
				.start(comprobarFichero)
				.next(crearDirectorioSalida)
				.next(procesarFichero)
				.next(archivarFichero)
				.listener(listenerJob)
				.build();

		// pattern matching: compruebo que es AbstractJob y hago el casting a la vez
		if (trabajoCompleto instanceof AbstractJob job) {
			totalPasos = job.getStepNames().size();
		}

		return trabajoCompleto;
	}
}
