package edu.pss.spring.config;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * PRÁCTICA: de los clientes procesados en el trabajo, obtener y guardar en el
 * contexto del JOB:
 *
 * 1) Número de localidades distintas
 * 2) Localidad con más clientes
 * 3) Localidad con el nombre más largo
 * 4) Porcentaje de pasos iniciados
 *
 * JOB - jobGestionFichero: comprobarFichero -> crearDirectorioSalida ->
 * procesarFichero -> archivarFichero
 */
@Configuration
public class ConfigBatch1 {

	// número de pasos del job, para calcular el porcentaje de pasos iniciados.
	// No lo escribimos a mano: lo calculamos al crear el job (ver jobGestionFichero)
	private int totalPasos;

	@Value("${batch.ruta.entrada}")
	private String rutaEntrada;

	@Value("${batch.ruta.procesados}")
	private String rutaProcesados;

	@Value("${batch.ruta.errores}")
	private String rutaErrores;

	@Value("${batch.fichero}")
	private String nombreFichero;

	// ===================== LISTENERS =====================

	/**
	 * Antes de cada paso: contamos los pasos iniciados y actualizamos el porcentaje
	 * (dato 4) en el contexto del JOB
	 */
	@Bean
	public StepExecutionListener listenerStep() {
		return new StepExecutionListener() {

			@Override
			public void beforeStep(StepExecution stepExecution) {
				System.out.println("-> Paso: " + stepExecution.getStepName());

				ExecutionContext jobContext = stepExecution.getJobExecution().getExecutionContext();

				int pasosIniciados = jobContext.getInt("pasosIniciados", 0) + 1;
				jobContext.put("pasosIniciados", pasosIniciados);
				jobContext.put("totalPasos", totalPasos);
				jobContext.put("porcentajePasosIniciados", pasosIniciados * 100.0 / totalPasos);
			}
		};
	}

	/**
	 * Al terminar el JOB: leemos los 4 datos del contexto del JOB y los mostramos
	 */
	@Bean
	public JobExecutionListener listenerJob() {
		return new JobExecutionListener() {

			@Override
			public void afterJob(JobExecution jobExecution) {
				ExecutionContext jobContext = jobExecution.getExecutionContext();

				System.out.println();
				System.out.println("========== RESULTADO DE LA PRÁCTICA ==========");
				System.out.println("Fichero: " + nombreFichero + "  |  Estado del job: " + jobExecution.getStatus());
				System.out.println();
				System.out.println("1) Número de localidades distintas: "
						+ jobContext.getInt("numLocalidades", 0));
				System.out.println("2) Localidad con más clientes: "
						+ jobContext.getString("localidadMasClientes", "NO CALCULADO"));
				System.out.println("3) Localidad con el nombre más largo: "
						+ jobContext.getString("localidadNombreMasLargo", "NO CALCULADO"));
				System.out.println("4) Porcentaje de pasos iniciados: "
						+ jobContext.getDouble("porcentajePasosIniciados", 0.0) + " % ("
						+ jobContext.getInt("pasosIniciados", 0) + " de " + jobContext.getInt("totalPasos", 0) + ")");
				System.out.println("==============================================");
				System.out.println();
			}
		};
	}

	// ===================== PASOS =====================

	@Bean
	public Step comprobarFichero(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			StepExecutionListener listenerStep) {
		return new StepBuilder("comprobarFichero", jobRepository).tasklet((contribution, chunkContext) -> {

			Path fichero = Paths.get(rutaEntrada, nombreFichero);

			if (!Files.exists(fichero)) {
				throw new RuntimeException("No existe el fichero " + fichero);
			}

			return RepeatStatus.FINISHED;
		}, transactionManager).listener(listenerStep).build();
	}

	@Bean
	public Step crearDirectorioSalida(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			StepExecutionListener listenerStep) {
		return new StepBuilder("crearDirectorioSalida", jobRepository).tasklet((contribution, chunkContext) -> {

			Files.createDirectories(Paths.get(rutaProcesados));
			Files.createDirectories(Paths.get(rutaErrores));

			return RepeatStatus.FINISHED;
		}, transactionManager).listener(listenerStep).build();
	}

	@Bean
	public Step procesarFichero(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			StepExecutionListener listenerStep) {
		return new StepBuilder("procesarFichero", jobRepository).tasklet((contribution, chunkContext) -> {

			List<String> lineas = Files.readAllLines(Paths.get(rutaEntrada, nombreFichero));

			List<String> lineasSalida = new ArrayList<>();
			List<String> lineasErrores = new ArrayList<>();

			// LOCALIDAD -> nº de clientes (LinkedHashMap mantiene el orden del fichero)
			Map<String, Integer> clientesPorLocalidad = new LinkedHashMap<>();

			if (!lineas.isEmpty()) {
				lineasSalida.add(lineas.get(0)); // cabecera
			}

			for (int i = 1; i < lineas.size(); i++) {
				String[] campos = lineas.get(i).split(";");

				if (campos.length != 4) {
					lineasErrores.add(lineas.get(i));
				} else {
					String id = campos[0].trim();
					String nombre = campos[1].trim().toUpperCase();
					String email = campos[2].trim().toLowerCase();
					String ciudad = campos[3].trim().toUpperCase();

					lineasSalida.add(id + ";" + nombre + ";" + email + ";" + ciudad + ";true");

					// sumamos 1 cliente a su localidad
					clientesPorLocalidad.put(ciudad, clientesPorLocalidad.getOrDefault(ciudad, 0) + 1);
				}
			}

			Files.write(Paths.get(rutaProcesados, "clis_procesados.csv"), lineasSalida, StandardCharsets.UTF_8);

			if (!lineasErrores.isEmpty()) {
				Files.write(Paths.get(rutaErrores, "clis_error.csv"), lineasErrores, StandardCharsets.UTF_8);
			}

			// LOCALIDAD -> longitud de su nombre
			Map<String, Integer> longitudPorLocalidad = new LinkedHashMap<>();
			for (String localidad : clientesPorLocalidad.keySet()) {
				longitudPorLocalidad.put(localidad, localidad.length());
			}

			// guardamos los 3 datos en el contexto del JOB
			ExecutionContext jobContext = contribution.getStepExecution().getJobExecution().getExecutionContext();

			jobContext.put("numLocalidades", clientesPorLocalidad.size());
			jobContext.put("localidadMasClientes", buscarMaximo(clientesPorLocalidad, "cliente", "clientes"));
			jobContext.put("localidadNombreMasLargo", buscarMaximo(longitudPorLocalidad, "carácter", "caracteres"));

			return RepeatStatus.FINISHED;
		}, transactionManager).listener(listenerStep).build();
	}

	@Bean
	public Step archivarFichero(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			StepExecutionListener listenerStep) {
		return new StepBuilder("archivarFichero", jobRepository).tasklet((contribution, chunkContext) -> {
			return RepeatStatus.FINISHED;
		}, transactionManager).listener(listenerStep).build();
	}

	// ===================== JOB =====================

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

		// getStepNames() sólo existe en la clase padre AbstractJob, no en el interfaz Job.
		// Pattern matching: compruebo que es un AbstractJob y hago el casting a la vez
		if (trabajoCompleto instanceof AbstractJob job) {
			totalPasos = job.getStepNames().size();
		}

		return trabajoCompleto;
	}

	// ===================== AUXILIAR =====================

	/**
	 * Devuelve la localidad con el valor más alto del mapa. Si varias empatan,
	 * avisa de que hay varias y devuelve solo la primera en el orden del fichero.
	 *
	 * Ejemplos: "MADRID (3 clientes)"
	 *           "MADRID (2 clientes) - hay 2 localidades empatadas, se muestra la primera"
	 */
	private static String buscarMaximo(Map<String, Integer> valores, String singular, String plural) {
		if (valores.isEmpty()) {
			return "NINGUNA";
		}

		int maximo = 0;
		List<String> ganadoras = new ArrayList<>();

		for (Map.Entry<String, Integer> entrada : valores.entrySet()) {
			if (entrada.getValue() > maximo) {
				maximo = entrada.getValue(); // nuevo máximo: empezamos la lista de nuevo
				ganadoras.clear();
				ganadoras.add(entrada.getKey());
			} else if (entrada.getValue() == maximo) {
				ganadoras.add(entrada.getKey()); // empate con el máximo
			}
		}

		// la primera ganadora en el orden del fichero (el LinkedHashMap mantiene ese orden)
		String resultado = ganadoras.get(0) + " (" + maximo + " " + (maximo == 1 ? singular : plural) + ")";

		if (ganadoras.size() > 1) {
			resultado = resultado + " - hay " + ganadoras.size() + " localidades empatadas, se muestra la primera";
		}

		return resultado;
	}
}
