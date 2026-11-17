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

/**
 * PRÁCTICA: LOCALIDADES DE LOS CLIENTES
 *
 * Qué pide el enunciado: a partir de los clientes del fichero, sacar estos 4 datos
 * y dejarlos guardados en el contexto del trabajo (JOB) para poder consultarlos al final:
 *
 *   1) Cuántas localidades distintas hay en el fichero
 *   2) Qué localidad tiene más clientes
 *   3) Qué localidad tiene el nombre más largo
 *   4) Qué porcentaje de los pasos del trabajo se han llegado a iniciar
 *
 * Cómo está montado: hay un trabajo (JOB) con 4 pasos (STEPS) que se ejecutan uno detrás de otro:
 *
 *   PASO 1 - comprobarFichero       -> mira que el fichero de clientes existe
 *   PASO 2 - crearDirectorioSalida  -> no hace nada, está para seguir el ejemplo de clase
 *   PASO 3 - procesarFichero        -> aquí se calculan los datos 1, 2 y 3
 *   PASO 4 - archivarFichero        -> no hace nada, está para seguir el ejemplo de clase
 *
 * Además hay dos "escuchadores" (listeners), que son trozos de código que Spring ejecuta
 * solos en momentos concretos:
 *
 *   - listenerStep: se ejecuta justo ANTES de empezar cada paso. Lo uso para el dato 4.
 *   - listenerJob:  se ejecuta cuando el trabajo TERMINA. Lo uso para enseñar los 4 datos.
 *
 * El "contexto del JOB" es como una mochila que el trabajo lleva encima de principio a fin:
 * cualquier paso o listener puede meter un dato en ella (put) y otro puede sacarlo después (get).
 * Así es como los datos que calculo en un paso llegan hasta el final del trabajo.
 *
 * Con @Configuration le digo a Spring que en esta clase están las piezas (beans) que
 * tiene que crear al arrancar la aplicación.
 */
@Configuration
public class ConfigBatch1 {

	// Con @Value, Spring va al fichero application.properties, busca la propiedad que pongo
	// entre llaves y copia su valor en la variable. Así la ruta y el nombre del fichero no
	// están escritos a mano en el código: si cambian, solo toco application.properties.

	// Carpeta donde está el fichero de clientes (en application.properties vale "datos/entrada")
	@Value("${batch.ruta.entrada}")
	private String rutaEntrada;

	// Nombre del fichero de clientes (en application.properties vale "clientes.csv")
	@Value("${batch.fichero}")
	private String nombreFichero;

	// Cuántos pasos tiene el trabajo en total. No lo escribo a mano (no pongo un 4):
	// lo calcula el propio trabajo cuando se crea, al final de esta clase (método jobGestionFichero).
	// Lo necesito para calcular el porcentaje del dato 4.
	private int totalPasos;

	// =====================================================================================
	// LISTENER DE LOS PASOS (DATO 4)
	// =====================================================================================

	/**
	 * Este listener lo tienen enganchado los 4 pasos (ver el .listener(listenerStep) de cada uno).
	 * Por eso Spring lo ejecuta 4 veces: una justo antes de que empiece cada paso.
	 *
	 * Cada vez que se ejecuta, apunta en la mochila del trabajo que se ha iniciado un paso más
	 * y recalcula el porcentaje. Si un paso falla, los siguientes ya no empiezan, así que el
	 * porcentaje se queda en el último valor (por ejemplo, si falla el primero, se queda en 25%).
	 */
	@Bean
	public StepExecutionListener listenerStep() {
		return new StepExecutionListener() {

			// beforeStep = "antes del paso". Spring me pasa la información del paso que va a empezar.
			@Override
			public void beforeStep(StepExecution stepExecution) {

				// Desde el paso subo hasta el trabajo y cojo su mochila (su contexto)
				ExecutionContext jobContexto = stepExecution.getJobExecution().getExecutionContext();

				// Miro cuántos pasos llevaba apuntados y le sumo 1 (este que empieza ahora).
				// La primera vez todavía no hay nada apuntado, por eso pongo un 0 por defecto.
				int stepsIniciados = jobContexto.getInt("stepsIniciados", 0) + 1;

				// Vuelvo a guardar la cuenta en la mochila para que el siguiente paso siga sumando
				jobContexto.put("stepsIniciados", stepsIniciados);

				// DATO 4: porcentaje de pasos iniciados = pasos iniciados x 100 / pasos totales
				// Pongo 100.0 (con decimal) y no 100 para que la división dé decimales (25.0, 50.0...)
				jobContexto.put("porcentajePasos", stepsIniciados * 100.0 / totalPasos);
			}
		};
	}

	// =====================================================================================
	// LISTENER DEL TRABAJO (ENSEÑAR LOS 4 DATOS AL FINAL)
	// =====================================================================================

	/**
	 * Este listener está enganchado al trabajo (ver el .listener(listenerJob) del final).
	 * Spring lo ejecuta una sola vez, cuando el trabajo termina, haya ido bien o mal.
	 *
	 * Lo único que hace es sacar de la mochila los 4 datos y escribirlos en la consola.
	 */
	@Bean
	public JobExecutionListener listenerJob() {
		return new JobExecutionListener() {

			// afterJob = "después del trabajo". Spring me pasa la información del trabajo terminado.
			@Override
			public void afterJob(JobExecution jobExecution) {

				// Cojo la mochila del trabajo, donde los pasos han ido dejando los datos
				ExecutionContext jobContexto = jobExecution.getExecutionContext();

				// Saco cada dato y lo escribo. El segundo valor de cada get (0, "", 0.0) es lo que
				// se escribe si el dato no está en la mochila, que pasa si el trabajo falló antes
				// de llegar al paso 3 (por ejemplo, porque el fichero no existía).
				System.out.println("1) Localidades distintas: " + jobContexto.getInt("numLocalidades", 0));
				System.out.println("2) Localidad con más clientes: " + jobContexto.getString("localidadMasClientes", ""));
				System.out.println("3) Localidad nombre más largo: " + jobContexto.getString("localidadMasLarga", ""));
				System.out.println("4) Porcentaje de pasos iniciados: " + jobContexto.getDouble("porcentajePasos", 0.0) + " %");
			}
		};
	}

	// =====================================================================================
	// PASO 1: COMPROBAR QUE EL FICHERO EXISTE
	// =====================================================================================

	/**
	 * Primer paso del trabajo. Comprueba que el fichero de clientes está donde tiene que estar.
	 * Si no está, no tiene sentido seguir, así que lanzo un error y el trabajo se para aquí.
	 *
	 * Todos los pasos están hechos igual que en clase: con un StepBuilder al que le doy un nombre,
	 * y con un "tasklet", que es el trozo de código que se ejecuta en ese paso.
	 */
	@Bean
	public Step comprobarFichero(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			StepExecutionListener listenerStep) {
		return new StepBuilder("comprobarFichero", jobRepository)
				.tasklet((StepContribution contribution, ChunkContext chunkContext) -> {

					// Junto la carpeta y el nombre para tener la ruta completa: datos/entrada/clientes.csv
					Path fichero = Paths.get(this.rutaEntrada, this.nombreFichero);

					// Si el fichero NO existe, lanzo un error con un mensaje que dice qué fichero falta.
					// Al lanzar el error, el trabajo termina como FAILED y los pasos siguientes no se hacen.
					if (!Files.exists(fichero)) {
						throw new RuntimeException("No existe el fichero " + fichero);
					}

					// FINISHED significa "este paso ha terminado bien, pasa al siguiente"
					return RepeatStatus.FINISHED;

				// Le paso el gestor de transacciones (como en clase) y le engancho el listener de los pasos
				}, transactionManager).listener(listenerStep).build();
	}

	// =====================================================================================
	// PASO 2: CREAR DIRECTORIO DE SALIDA
	// =====================================================================================

	/**
	 * Segundo paso. En esta práctica no hace nada porque el enunciado no pide escribir ficheros,
	 * pero lo dejo porque forma parte del trabajo que hicimos en clase.
	 * Además cuenta para el porcentaje de pasos iniciados (dato 4).
	 */
	@Bean
	public Step crearDirectorioSalida(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			StepExecutionListener listenerStep) {
		return new StepBuilder("crearDirectorioSalida", jobRepository)
				.tasklet((StepContribution contribution, ChunkContext chunkContext) -> {
					// No hace nada: solo dice que ha terminado bien
					return RepeatStatus.FINISHED;
				}, transactionManager).listener(listenerStep).build();
	}

	// =====================================================================================
	// PASO 3: PROCESAR EL FICHERO (DATOS 1, 2 Y 3)
	// =====================================================================================

	/**
	 * Tercer paso: es la parte principal de la práctica.
	 *
	 * Lee el fichero de clientes, se queda con la ciudad de cada cliente, calcula los datos
	 * 1, 2 y 3 y los guarda en la mochila del trabajo.
	 *
	 * Cada línea del fichero tiene esta forma:  id;nombre;email;ciudad
	 * Por ejemplo:                              1;Ana García;ana.garcia@email.com;Madrid
	 */
	@Bean
	public Step procesarFichero(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			StepExecutionListener listenerStep) {
		return new StepBuilder("procesarFichero", jobRepository)
				.tasklet((StepContribution contribution, ChunkContext chunkContext) -> {

					// ---------- Leer el fichero ----------

					// Leo el fichero entero de golpe. Me queda una lista en la que cada elemento
					// es una línea del fichero (la primera línea es la cabecera: id;nombre;email;ciudad)
					List<String> lineas = Files.readAllLines(Paths.get(this.rutaEntrada, this.nombreFichero));

					// ---------- Sacar la ciudad de cada cliente ----------

					// En esta lista voy a guardar la ciudad de cada cliente que esté bien escrito.
					// Una ciudad puede salir varias veces (si tiene varios clientes): eso lo necesito
					// después para contar cuántos clientes tiene cada una.
					List<String> ciudades = new ArrayList<>();

					// Recorro las líneas empezando en la 1 y no en la 0, para saltarme la cabecera
					for (int indice = 1; indice < lineas.size(); indice++) {

						// Parto la línea por los punto y coma. Me queda un array con los trozos:
						// campos[0] = id, campos[1] = nombre, campos[2] = email, campos[3] = ciudad
						String[] campos = lineas.get(indice).split(";");

						// Solo cuento los clientes que tienen exactamente 4 datos.
						// Si una línea tiene más o menos, está mal escrita y la ignoro.
						if (campos.length == 4) {

							// Me quedo con la ciudad. Antes de guardarla:
							//  - trim() le quita los espacios que pueda tener delante o detrás (" madrid" -> "madrid")
							//  - toUpperCase() la pone en mayúsculas ("madrid" -> "MADRID")
							// Así "Madrid", " madrid" y "MADRID" cuentan como la misma ciudad.
							ciudades.add(campos[3].trim().toUpperCase());
						}
					}

					// ---------- DATO 1: localidades distintas ----------

					// Hago una segunda lista con cada ciudad UNA sola vez (sin repetidas)
					List<String> distintas = new ArrayList<>();

					// Recorro todas las ciudades y añado cada una solo si todavía no estaba en la lista
					for (String ciudad : ciudades) {
						if (!distintas.contains(ciudad)) {
							distintas.add(ciudad);
						}
					}
					// Al terminar, el número de localidades distintas es el tamaño de esta lista: distintas.size()

					// ---------- DATO 2 y DATO 3: los calculo en el mismo recorrido ----------

					// Para los dos uso la misma idea: me quedo con "la mejor encontrada hasta ahora"
					// y solo la cambio si aparece una mejor.

					String masClientes = "";   // la localidad con más clientes encontrada hasta ahora
					int maxClientes = 0;       // cuántos clientes tiene esa localidad
					String masLarga = "";      // la localidad con el nombre más largo encontrada hasta ahora

					// Recorro cada localidad distinta (cada una una sola vez)
					for (String localidad : distintas) {

						// DATO 2: cuento cuántas veces aparece esta localidad en la lista de todas
						// las ciudades. Cada vez que aparece es un cliente más de esa localidad.
						int clientes = 0;
						for (String ciudad : ciudades) {
							// Para comparar textos uso equals (con == no funciona bien con los String)
							if (ciudad.equals(localidad)) {
								clientes++;
							}
						}

						// Si esta localidad tiene MÁS clientes que la mejor que llevaba, pasa a ser la mejor.
						// Si tiene los mismos (empate), no la cambio: se queda la que apareció antes.
						if (clientes > maxClientes) {
							maxClientes = clientes;
							masClientes = localidad;
						}

						// DATO 3: si el nombre de esta localidad tiene MÁS letras que el de la mejor
						// que llevaba, pasa a ser la mejor. length() cuenta las letras (y los espacios).
						if (localidad.length() > masLarga.length()) {
							masLarga = localidad;
						}
					}

					// ---------- Guardar los datos en la mochila del trabajo ----------

					// Desde el paso subo hasta el trabajo para coger SU mochila (no la del paso),
					// porque el enunciado pide que los datos estén en el contexto del JOB.
					// El camino es: contribution -> paso (StepExecution) -> trabajo (JobExecution) -> su contexto
					ExecutionContext jobContexto = contribution.getStepExecution().getJobExecution().getExecutionContext();

					// Guardo cada dato con un nombre, para que el listenerJob pueda sacarlo al final
					jobContexto.put("numLocalidades", distintas.size());     // DATO 1
					jobContexto.put("localidadMasClientes", masClientes);    // DATO 2
					jobContexto.put("localidadMasLarga", masLarga);          // DATO 3

					// Paso terminado bien, pasa al siguiente
					return RepeatStatus.FINISHED;

				}, transactionManager).listener(listenerStep).build();
	}

	// =====================================================================================
	// PASO 4: ARCHIVAR EL FICHERO
	// =====================================================================================

	/**
	 * Cuarto y último paso. Igual que el paso 2, no hace nada en esta práctica, pero lo dejo
	 * porque forma parte del trabajo de clase y cuenta para el porcentaje de pasos (dato 4).
	 */
	@Bean
	public Step archivarFichero(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			StepExecutionListener listenerStep) {
		return new StepBuilder("archivarFichero", jobRepository)
				.tasklet((StepContribution contribution, ChunkContext chunkContext) -> {
					// No hace nada: solo dice que ha terminado bien
					return RepeatStatus.FINISHED;
				}, transactionManager).listener(listenerStep).build();
	}

	// =====================================================================================
	// EL TRABAJO (JOB): JUNTA LOS 4 PASOS
	// =====================================================================================

	/**
	 * Aquí se monta el trabajo completo. Spring me pasa los 4 pasos que he creado arriba
	 * (los reconoce porque los parámetros se llaman igual que los métodos) y el listener del trabajo.
	 *
	 * Con start y next digo en qué orden se ejecutan los pasos.
	 * Si un paso falla, los que vienen detrás ya no se ejecutan.
	 */
	@Bean
	public Job jobGestionFichero(JobRepository jobRepository, Step comprobarFichero, Step crearDirectorioSalida,
			Step procesarFichero, Step archivarFichero, JobExecutionListener listenerJob) {

		Job trabajoCompleto = new JobBuilder("jobGestionFichero", jobRepository)
				.start(comprobarFichero)        // paso 1
				.next(crearDirectorioSalida)    // paso 2
				.next(procesarFichero)          // paso 3
				.next(archivarFichero)          // paso 4
				.listener(listenerJob)          // al terminar, se ejecuta el listener que enseña los datos
				.build();

		// Aquí cuento cuántos pasos tiene el trabajo, para el porcentaje del dato 4.
		// El método que da los nombres de los pasos (getStepNames) no está en Job, sino en su
		// clase padre AbstractJob. Por eso uso "pattern matching", como en clase: el instanceof
		// comprueba que el trabajo es un AbstractJob y, a la vez, me lo da ya convertido en "job".
		if (trabajoCompleto instanceof AbstractJob job) {
			// Cuántos nombres de pasos hay = cuántos pasos tiene el trabajo (4)
			totalPasos = job.getStepNames().size();
		}

		return trabajoCompleto;
	}
}
