import {
	Edition,
	createDefaultBatchTranslationState,
	isBatchProjectSegmentationVerified,
	type Batch,
	type BatchProjectItem,
	type BatchProjectTranslationState,
	type Project
} from '$lib/classes';
import { globalState } from '$lib/runes/main.svelte';
import { BatchService } from './BatchService';
import { ProjectService } from './ProjectService';
import {
	fetchTranslationsFromOtherProjects,
	getProjectSubtitleClips,
	getProjectTranslationReviewCounts
} from './TranslationFetchService';
import { runBatchWorkerPool } from './BatchWorkerPool';
import type { AITranslationSettings } from '$lib/classes/Settings.svelte';
import {
	applyAiSubtitleSplitValidationSuccess,
	buildAiSubtitleSplitBatches,
	buildAiSubtitleSplitCandidates,
	runAiSubtitleSplitBatchStreaming,
	validateAiSubtitleSplitBatchResult
} from './AiSubtitleSplittingService';
import {
	applyAdvancedTrimValidationSuccess,
	buildAdvancedTrimBatches,
	buildAdvancedTrimVerseCandidates,
	runAdvancedTrimBatchStreaming,
	validateAdvancedTrimBatchResult
} from './AdvancedAITrimming';

export const BATCH_TRANSLATION_CONCURRENCY = 3;

export type BatchTranslationActivity = 'adding' | 'fetching' | 'splitting' | 'trimming' | 'saving';

export interface BatchTranslationServiceOptions {
	onUpdate?: (
		item: BatchProjectItem,
		editionName: string,
		activity: BatchTranslationActivity
	) => void;
	onProgress?: (progress: BatchTranslationQueueProgress) => void;
}

export interface BatchTranslationQueueProgress {
	active: number;
	completed: number;
	failed: number;
	skipped: number;
	remaining: number;
	progress: number;
	total: number;
}

export interface BatchTranslationRunResult {
	completed: number;
	failed: number;
	skipped: number;
}

/**
 * Copie une édition afin que chaque projet sauvegarde sa propre instance.
 * @param {Edition} edition Édition source.
 * @returns {Edition} Nouvelle instance équivalente.
 */
function cloneEdition(edition: Edition): Edition {
	return new Edition(
		edition.key,
		edition.name,
		edition.author,
		edition.language,
		edition.direction,
		edition.source,
		edition.comments,
		edition.link,
		edition.linkmin,
		edition.showInTranslationsEditor
	);
}

/**
 * Synchronise la visibilité d'une édition dans tous les projets d'un Batch.
 * @param {number} batchId Identifiant du Batch parent.
 * @param {string} editionName Nom de l'édition modifiée.
 * @param {boolean} visible Visibilité à appliquer à l'édition.
 * @param {boolean} exclusive Masque également toutes les autres éditions.
 * @param {Project | null} currentProject Projet déjà chargé à réutiliser.
 * @returns {Promise<void>} Résolution après la sauvegarde des projets modifiés.
 */
export async function synchronizeBatchTranslationEditorVisibility(
	batchId: number,
	editionName: string,
	visible: boolean,
	exclusive: boolean = false,
	currentProject: Project | null = globalState.currentProject
): Promise<void> {
	const batch = await BatchService.load(batchId);
	for (const item of batch.projects) {
		const project =
			currentProject?.detail.id === item.projectId
				? currentProject
				: await ProjectService.load(item.projectId);
		let changed = false;
		for (const edition of project.content.projectTranslation.addedTranslationEditions) {
			if (!exclusive && edition.name !== editionName) continue;
			const nextVisible = edition.name === editionName ? visible : false;
			if (edition.showInTranslationsEditor === nextVisible) continue;
			edition.showInTranslationsEditor = nextVisible;
			changed = true;
		}
		if (changed) await ProjectService.save(project);
	}
}

/**
 * Synchronise les filtres de traduction du projet courant dans tout son Batch.
 * @param {number} batchId Identifiant du Batch parent.
 * @param {Project} sourceProject Projet fournissant les filtres à partager.
 * @returns {Promise<void>} Résolution après la sauvegarde des projets modifiés.
 */
export async function synchronizeBatchTranslationEditorFilters(
	batchId: number,
	sourceProject: Project
): Promise<void> {
	const source = sourceProject.projectEditorState.translationsEditor;
	const filters = { ...source.filters };
	const batch = await BatchService.load(batchId);
	for (const item of batch.projects) {
		const project =
			sourceProject.detail.id === item.projectId
				? sourceProject
				: await ProjectService.load(item.projectId);
		const target = project.projectEditorState.translationsEditor;
		const changed =
			JSON.stringify(target.filters) !== JSON.stringify(filters) ||
			target.searchQuery !== source.searchQuery ||
			target.onlyShowOverlappingSubtitles !== source.onlyShowOverlappingSubtitles;
		if (changed) {
			target.filters = { ...filters };
			target.searchQuery = source.searchQuery;
			target.onlyShowOverlappingSubtitles = source.onlyShowOverlappingSubtitles;
		}
		if (changed || project === sourceProject) await ProjectService.save(project);
	}
}

export class BatchTranslationService {
	private readonly onUpdate?: BatchTranslationServiceOptions['onUpdate'];
	private readonly onProgress?: BatchTranslationServiceOptions['onProgress'];
	private saveChain: Promise<void> = Promise.resolve();

	/**
	 * Crée le service d'orchestration des traductions Batch.
	 * @param {BatchTranslationServiceOptions} options Callback UI éventuel.
	 */
	constructor(options: BatchTranslationServiceOptions = {}) {
		this.onUpdate = options.onUpdate;
		this.onProgress = options.onProgress;
	}

	/**
	 * Sérialise les sauvegardes concurrentes du manifeste.
	 * @param {Batch} batch Manifeste à persister.
	 * @returns {Promise<void>} Résolution après l'écriture correspondante.
	 */
	private saveBatch(batch: Batch): Promise<void> {
		batch.updatedAt = new Date();
		this.saveChain = this.saveChain.then(() => BatchService.save(batch));
		return this.saveChain;
	}

	/**
	 * Retourne ou initialise le résumé d'une édition pour une ligne Batch.
	 * @param {BatchProjectItem} item Ligne cible.
	 * @param {Edition} edition Édition suivie.
	 * @returns {BatchProjectTranslationState} État persistant de l'édition.
	 */
	private getState(item: BatchProjectItem, edition: Edition): BatchProjectTranslationState {
		return (item.translations[edition.name] ??= createDefaultBatchTranslationState({
			editionName: edition.name,
			editionAuthor: edition.author,
			editionLanguage: edition.language
		}));
	}

	/**
	 * Ajoute plusieurs éditions aux projets avec trois workers au maximum.
	 * @param {Batch} batch Manifeste parent.
	 * @param {BatchProjectItem[]} items Projets sélectionnés dans l'ordre du Batch.
	 * @param {Edition[]} editions Éditions choisies.
	 * @param {boolean} skipExisting Ignore les éditions déjà présentes.
	 * @returns {Promise<BatchTranslationRunResult>} Résumé de la queue.
	 */
	async addEditions(
		batch: Batch,
		items: BatchProjectItem[],
		editions: Edition[],
		skipExisting: boolean = true
	): Promise<BatchTranslationRunResult> {
		const result: BatchTranslationRunResult = { completed: 0, failed: 0, skipped: 0 };
		const total = items.length * editions.length;
		/**
		 * Publie l'avancement agrégé en opérations projet × édition.
		 * @returns {void}
		 */
		const reportProgress = (): void => {
			const processed = result.completed + result.failed + result.skipped;
			this.onProgress?.({
				active: 0,
				completed: result.completed,
				failed: result.failed,
				skipped: result.skipped,
				remaining: Math.max(total - processed, 0),
				progress: Math.round((processed / Math.max(total, 1)) * 100),
				total
			});
		};
		reportProgress();
		await runBatchWorkerPool(items, BATCH_TRANSLATION_CONCURRENCY, async (item) => {
			if (!isBatchProjectSegmentationVerified(item)) {
				result.skipped += editions.length;
				reportProgress();
				return;
			}
			try {
				const project = await ProjectService.load(item.projectId);
				if (getProjectSubtitleClips(project).length === 0) {
					result.skipped += editions.length;
					reportProgress();
					return;
				}
				for (const sourceEdition of editions) {
					const edition = cloneEdition(sourceEdition);
					const exists = project.content.projectTranslation.addedTranslationEditions.some(
						(candidate) => candidate.name === edition.name
					);
					if (exists && skipExisting) {
						const state = this.getState(item, edition);
						state.review = getProjectTranslationReviewCounts(project, edition.name);
						state.status = state.review.pending === 0 ? 'auto_verified' : 'ready_to_fetch';
						state.progress = 100;
						result.skipped++;
						reportProgress();
						await this.saveBatch(batch);
						this.onUpdate?.(item, edition.name, 'saving');
						continue;
					}
					const state = this.getState(item, edition);
					state.status = 'adding';
					state.progress = 0;
					state.error = null;
					this.onUpdate?.(item, edition.name, 'adding');
					await this.saveBatch(batch);

					const translations =
						await project.content.projectTranslation.getAllProjectSubtitlesTranslationsForProject(
							project,
							edition
						);
					state.progress = 80;
					this.onUpdate?.(item, edition.name, 'adding');
					await project.content.projectTranslation.addTranslationToProject(
						project,
						edition,
						translations,
						{ replaceExisting: exists && !skipExisting }
					);
					state.progress = 95;
					this.onUpdate?.(item, edition.name, 'saving');
					await project.save();
					globalState.userProjectsDetails = [
						project.detail,
						...globalState.userProjectsDetails.filter((detail) => detail.id !== project.detail.id)
					];
					state.review = getProjectTranslationReviewCounts(project, edition.name);
					state.status = state.review.pending === 0 ? 'auto_verified' : 'ready_to_fetch';
					state.progress = 100;
					state.addedAt ??= new Date();
					state.completedAt = state.review.pending === 0 ? new Date() : null;
					result.completed++;
					reportProgress();
					await this.saveBatch(batch);
					this.onUpdate?.(item, edition.name, 'saving');
				}
			} catch (error) {
				for (const edition of editions) {
					const state = this.getState(item, edition);
					if (!['not_added', 'adding'].includes(state.status)) continue;
					state.status = 'failed';
					state.error = String(error);
					state.progress = 0;
					this.onUpdate?.(item, edition.name, 'adding');
					result.failed++;
				}
				reportProgress();
				await this.saveBatch(batch);
			}
		});
		await this.saveChain;
		return result;
	}

	/**
	 * Fetch une édition précise dans les projets avec trois workers au maximum.
	 * @param {Batch} batch Manifeste parent.
	 * @param {BatchProjectItem[]} items Projets sélectionnés.
	 * @param {string} editionName Édition cible unique.
	 * @returns {Promise<BatchTranslationRunResult>} Résumé de la queue.
	 */
	async fetchEdition(
		batch: Batch,
		items: BatchProjectItem[],
		editionName: string
	): Promise<BatchTranslationRunResult> {
		const result: BatchTranslationRunResult = { completed: 0, failed: 0, skipped: 0 };
		const progressByProject = new Map(items.map((item) => [item.projectId, 0]));
		let active = 0;
		/**
		 * Publie l'avancement agrégé des projets de cette exécution uniquement.
		 * @returns {void}
		 */
		const reportProgress = (): void => {
			const processed = result.completed + result.failed + result.skipped;
			this.onProgress?.({
				active,
				completed: result.completed,
				failed: result.failed,
				skipped: result.skipped,
				remaining: Math.max(items.length - active - processed, 0),
				progress: Math.round(
					[...progressByProject.values()].reduce((sum, progress) => sum + progress, 0) /
						Math.max(items.length, 1)
				),
				total: items.length
			});
		};
		reportProgress();
		if (globalState.userProjectsDetails.length === 0)
			await ProjectService.loadUserProjectsDetails();
		await runBatchWorkerPool(items, BATCH_TRANSLATION_CONCURRENCY, async (item) => {
			const state = item.translations[editionName];
			if (!state) {
				result.skipped++;
				progressByProject.set(item.projectId, 100);
				reportProgress();
				return;
			}
			active++;
			reportProgress();
			try {
				const project = await ProjectService.load(item.projectId);
				const edition = project.content.projectTranslation.addedTranslationEditions.find(
					(candidate) => candidate.name === editionName
				);
				if (!edition) {
					result.skipped++;
					return;
				}
				state.status = 'fetching';
				state.progress = 0;
				state.error = null;
				this.onUpdate?.(item, editionName, 'fetching');
				await this.saveBatch(batch);
				const fetchResult = await fetchTranslationsFromOtherProjects({
					targetProject: project,
					edition,
					sourceProjectDetails: globalState.userProjectsDetails,
					onProgress: (progress) => {
						state.progress = Math.round(progress * 0.9);
						progressByProject.set(item.projectId, state.progress);
						this.onUpdate?.(item, editionName, 'fetching');
						reportProgress();
					}
				});
				state.progress = 90;
				progressByProject.set(item.projectId, state.progress);
				state.review = fetchResult.review;
				this.onUpdate?.(item, editionName, 'fetching');
				project.content.projectTranslation.updateProjectPercentage(project, edition);
				state.progress = 98;
				progressByProject.set(item.projectId, state.progress);
				this.onUpdate?.(item, editionName, 'saving');
				await project.save();
				globalState.userProjectsDetails = [
					project.detail,
					...globalState.userProjectsDetails.filter((detail) => detail.id !== project.detail.id)
				];
				state.status = state.review.pending === 0 ? 'auto_verified' : 'needs_review';
				state.progress = 100;
				state.fetchedAt = new Date();
				state.completedAt = state.review.pending === 0 ? new Date() : null;
				await this.saveBatch(batch);
				this.onUpdate?.(item, editionName, 'saving');
				result.completed++;
			} catch (error) {
				state.status = 'failed';
				state.error = String(error);
				state.progress = 0;
				this.onUpdate?.(item, editionName, 'fetching');
				await this.saveBatch(batch);
				result.failed++;
			} finally {
				active--;
				progressByProject.set(item.projectId, 100);
				reportProgress();
			}
		});
		await this.saveChain;
		return result;
	}

	/**
	 * Découpe par IA les sous-titres longs de tous les projets fournis.
	 * @param {Batch} batch Manifeste parent.
	 * @param {BatchProjectItem[]} items Projets du Batch à traiter.
	 * @param {number} maxWords Nombre maximal de mots par sous-titre.
	 * @param {AITranslationSettings} aiSettings Configuration du fournisseur IA.
	 * @returns {Promise<BatchTranslationRunResult>} Résumé de la queue.
	 */
	async aiSplitLongSubtitles(
		batch: Batch,
		items: BatchProjectItem[],
		maxWords: number,
		aiSettings: AITranslationSettings
	): Promise<BatchTranslationRunResult> {
		const result: BatchTranslationRunResult = { completed: 0, failed: 0, skipped: 0 };
		let active = 0;
		/**
		 * Publie l'avancement agrégé des projets découpés.
		 * @returns {void}
		 */
		const reportProgress = (): void => {
			const processed = result.completed + result.failed + result.skipped;
			this.onProgress?.({
				active,
				completed: result.completed,
				failed: result.failed,
				skipped: result.skipped,
				remaining: Math.max(items.length - active - processed, 0),
				progress: Math.round((processed / Math.max(items.length, 1)) * 100),
				total: items.length
			});
		};
		reportProgress();
		await runBatchWorkerPool(items, BATCH_TRANSLATION_CONCURRENCY, async (item) => {
			active++;
			reportProgress();
			try {
				console.info('[Batch AI Split] Starting project', {
					projectId: item.projectId,
					projectName: item.projectName,
					maxWords
				});
				const project = await ProjectService.load(item.projectId);
				const candidates = await buildAiSubtitleSplitCandidates(maxWords, project);
				if (candidates.length === 0) {
					console.info('[Batch AI Split] No eligible subtitle', {
						projectId: item.projectId,
						projectName: item.projectName
					});
					result.skipped++;
					return;
				}

				const errors: string[] = [];
				let appliedSplits = 0;
				for (const [batchIndex, splitBatch] of buildAiSubtitleSplitBatches(candidates).entries()) {
					console.info('[Batch AI Split] Running AI batch', {
						projectId: item.projectId,
						batch: batchIndex + 1,
						segments: splitBatch.segments.length
					});
					const response = await runAiSubtitleSplitBatchStreaming({
						apiKey: aiSettings.openAiApiKey.trim(),
						endpoint: aiSettings.textAiApiEndpoint.trim(),
						model: aiSettings.advancedTrimModel,
						reasoningEffort: aiSettings.advancedTrimReasoningEffort,
						batchId: `batch-ai-split-${item.projectId}-${batchIndex + 1}`,
						batch: splitBatch.request
					});
					const validation = validateAiSubtitleSplitBatchResult(splitBatch, response.parsed);
					const applyReport = await applyAiSubtitleSplitValidationSuccess(
						validation.validSegments,
						project
					);
					appliedSplits += applyReport.appliedSplits;
					errors.push(...validation.errors, ...applyReport.errors);
				}

				if (appliedSplits > 0) {
					for (const edition of project.content.projectTranslation.addedTranslationEditions) {
						project.content.projectTranslation.updateProjectPercentage(project, edition);
						const state = item.translations[edition.name];
						if (!state) continue;
						state.review = getProjectTranslationReviewCounts(project, edition.name);
						state.status = state.review.pending === 0 ? 'auto_verified' : 'needs_review';
						state.progress = 100;
						state.error = null;
						state.completedAt = state.review.pending === 0 ? new Date() : null;
					}
					this.onUpdate?.(item, '', 'saving');
					await project.save();
					await this.saveBatch(batch);
				}
				if (errors.length > 0) throw new Error(errors[0]);
				console.info('[Batch AI Split] Project completed', {
					projectId: item.projectId,
					projectName: item.projectName,
					appliedSplits
				});
				result.completed++;
			} catch (error) {
				console.error('[Batch AI Split] Project failed', {
					projectId: item.projectId,
					projectName: item.projectName,
					error
				});
				result.failed++;
			} finally {
				active--;
				reportProgress();
			}
		});
		await this.saveChain;
		return result;
	}

	/**
	 * Lance le trimmer IA sur l'édition choisie dans tous les projets fournis.
	 * @param {Batch} batch Manifeste parent.
	 * @param {BatchProjectItem[]} items Projets du Batch à traiter.
	 * @param {string} editionName Nom de l'édition cible.
	 * @param {AITranslationSettings} aiSettings Configuration du fournisseur IA.
	 * @returns {Promise<BatchTranslationRunResult>} Résumé de la queue.
	 */
	async aiTrimEdition(
		batch: Batch,
		items: BatchProjectItem[],
		editionName: string,
		aiSettings: AITranslationSettings
	): Promise<BatchTranslationRunResult> {
		const result: BatchTranslationRunResult = { completed: 0, failed: 0, skipped: 0 };
		let active = 0;
		/**
		 * Publie l'avancement agrégé des projets trimmés.
		 * @returns {void}
		 */
		const reportProgress = (): void => {
			const processed = result.completed + result.failed + result.skipped;
			this.onProgress?.({
				active,
				completed: result.completed,
				failed: result.failed,
				skipped: result.skipped,
				remaining: Math.max(items.length - active - processed, 0),
				progress: Math.round((processed / Math.max(items.length, 1)) * 100),
				total: items.length
			});
		};
		reportProgress();
		await runBatchWorkerPool(items, BATCH_TRANSLATION_CONCURRENCY, async (item) => {
			active++;
			reportProgress();
			try {
				console.info('[Batch AI Trim] Starting project', {
					projectId: item.projectId,
					projectName: item.projectName,
					editionName
				});
				const project = await ProjectService.load(item.projectId);
				const edition = project.content.projectTranslation.addedTranslationEditions.find(
					(candidate) => candidate.name === editionName
				);
				const state = item.translations[editionName];
				if (!edition || !state) {
					console.info('[Batch AI Trim] Edition missing, skipping project', {
						projectId: item.projectId,
						projectName: item.projectName,
						editionName
					});
					result.skipped++;
					return;
				}
				const candidates = buildAdvancedTrimVerseCandidates(
					edition,
					aiSettings.advancedAlsoAskReviewed,
					project
				);
				if (candidates.length === 0) {
					console.info('[Batch AI Trim] No eligible translation', {
						projectId: item.projectId,
						projectName: item.projectName,
						editionName
					});
					result.skipped++;
					return;
				}

				this.onUpdate?.(item, editionName, 'trimming');
				const validationErrors: string[] = [];
				const reviewWarnings: string[] = [];
				let appliedSegments = 0;
				const trimBatches = buildAdvancedTrimBatches(
					candidates,
					aiSettings.advancedTrimModel,
					aiSettings.advancedTrimReasoningEffort,
					0,
					Number.MAX_SAFE_INTEGER
				);
				for (const [batchIndex, trimBatch] of trimBatches.entries()) {
					console.info('[Batch AI Trim] Running AI batch', {
						projectId: item.projectId,
						batch: batchIndex + 1,
						verses: trimBatch.verses.length
					});
					const response = await runAdvancedTrimBatchStreaming({
						apiKey: aiSettings.openAiApiKey.trim(),
						endpoint: aiSettings.textAiApiEndpoint.trim(),
						model: aiSettings.advancedTrimModel,
						reasoningEffort: aiSettings.advancedTrimReasoningEffort,
						batchId: `batch-ai-trim-${item.projectId}-${batchIndex + 1}`,
						batch: trimBatch.request
					});
					const validation = validateAdvancedTrimBatchResult(trimBatch, response.parsed);
					const applyReport = applyAdvancedTrimValidationSuccess(
						edition,
						validation.validVerses,
						project
					);
					appliedSegments += applyReport.appliedSegments;
					validationErrors.push(...validation.errors);
					reviewWarnings.push(...applyReport.errors);
				}
				if (reviewWarnings.length > 0) {
					console.warn('[Batch AI Trim] Segments require review', {
						projectId: item.projectId,
						projectName: item.projectName,
						editionName,
						warnings: reviewWarnings
					});
				}

				if (appliedSegments > 0) {
					project.content.projectTranslation.updateProjectPercentage(project, edition);
					state.review = getProjectTranslationReviewCounts(project, editionName);
					state.status = state.review.pending === 0 ? 'auto_verified' : 'needs_review';
					state.progress = 100;
					state.error = validationErrors[0] ?? null;
					state.completedAt = state.review.pending === 0 ? new Date() : null;
					this.onUpdate?.(item, editionName, 'saving');
					await project.save();
					await this.saveBatch(batch);
				}
				if (validationErrors.length > 0) throw new Error(validationErrors[0]);
				console.info('[Batch AI Trim] Project completed', {
					projectId: item.projectId,
					projectName: item.projectName,
					appliedSegments
				});
				result.completed++;
			} catch (error) {
				console.error('[Batch AI Trim] Project failed', {
					projectId: item.projectId,
					projectName: item.projectName,
					editionName,
					error
				});
				result.failed++;
			} finally {
				active--;
				reportProgress();
			}
		});
		await this.saveChain;
		return result;
	}
}

/**
 * Réconcilie uniquement les résumés de traduction susceptibles d'être obsolètes.
 * @param {Batch} batch Manifeste à vérifier.
 * @returns {Promise<boolean>} `true` si le manifeste a changé.
 */
export async function reconcileBatchTranslations(batch: Batch): Promise<boolean> {
	let changed = false;
	for (const item of batch.projects) {
		const states = Object.values(item.translations).filter(
			(state) =>
				state.status === 'ready_to_fetch' ||
				state.status === 'needs_review' ||
				state.review.total === 0 ||
				state.error === 'TRANSLATION_INTERRUPTED'
		);
		if (states.length === 0) continue;
		try {
			const project = await ProjectService.load(item.projectId);
			for (const state of states) {
				const edition = project.content.projectTranslation.addedTranslationEditions.find(
					(candidate) => candidate.name === state.editionName
				);
				if (!edition) continue;
				const review = getProjectTranslationReviewCounts(project, state.editionName);
				if (JSON.stringify(review) !== JSON.stringify(state.review)) {
					state.review = review;
					changed = true;
				}
				if (
					review.total > 0 &&
					(state.status === 'not_added' || state.error === 'TRANSLATION_INTERRUPTED')
				) {
					state.status = review.pending === 0 ? 'auto_verified' : 'ready_to_fetch';
					state.error = null;
					state.progress = 100;
					state.addedAt ??= new Date();
					state.completedAt = review.pending === 0 ? new Date() : null;
					changed = true;
				}
				if (state.status === 'ready_to_fetch' && review.pending === 0) {
					state.status = 'auto_verified';
					state.completedAt = new Date();
					changed = true;
				}
				if (state.status === 'needs_review' && review.pending === 0) {
					state.status = 'manually_verified';
					state.completedAt = new Date();
					changed = true;
				}
			}
		} catch {
			// Un projet illisible conserve son résumé; les actions afficheront l'erreur à l'exécution.
		}
	}
	if (changed) {
		batch.updatedAt = new Date();
		await BatchService.save(batch);
	}
	return changed;
}
