import express from 'express';
import sql from 'mssql';
import axios from 'axios';
import dotenv from 'dotenv';
import PQueue from 'p-queue';

dotenv.config();

// --- Configurações da Aplicação e Variáveis de Ambiente ---
const app = express();
const PORT = process.env.PORT || 3000;

const dbConfig = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    server: process.env.DB_SERVER,
    database: process.env.DB_DATABASE,
    options: { encrypt: true, trustServerCertificate: false }
};

const OMIE_APP_KEY = process.env.OMIE_APP_KEY;
const OMIE_APP_SECRET = process.env.OMIE_APP_SECRET;
const OMIE_BASE_URL_FINANCAS = 'https://app.omie.com.br/api/v1/financas/';

// Certifique-se que estes códigos existem e estão configurados corretamente na Omie
const CODIGO_FORNECEDOR_FIXO = "CodigoInterno01";
const CODIGO_CONTA_CORRENTE_FIXO = 10892094304;

// --- Configurações para o Controle de Taxa com p-queue ---
const OMIE_CONCURRENCY_LIMIT = 1; // Limita 1 requisição por vez
const OMIE_QUEUE_INTERVAL_MS = 260; // Espera 260ms entre o início de cada requisição
const OMIE_QUEUE_INTERVAL_CAP = 1; // Permite 1 requisição a cada OMIE_QUEUE_INTERVAL_MS

// Instância global da fila para gerenciar as requisições Omie
const omieQueue = new PQueue({
    concurrency: OMIE_CONCURRENCY_LIMIT,
    interval: OMIE_QUEUE_INTERVAL_MS,
    intervalCap: OMIE_QUEUE_INTERVAL_CAP,
});

// Variáveis de retry para safePost
const MAX_RETRIES = 5;
const BASE_RETRY_DELAY_MS = 1000;

let dbPool; // Pool de conexões do banco de dados

// --- Utilitários ---

/**
 * Formata uma data para o padrão DD/MM/YYYY (pt-BR).
 */
const formatDate = (dateInput) => {
    const date = new Date(dateInput);
    if (isNaN(date.getTime())) {
        return null; // Retorna null ou joga um erro se a data for inválida
    }
    return date.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric' });
};

/**
 * Adiciona dias a uma data e retorna no formato DD/MM/YYYY (pt-BR).
 */
const addDays = (dateInput, days) => {
    const date = new Date(dateInput && !isNaN(new Date(dateInput).getTime()) ? dateInput : new Date());
    date.setDate(date.getDate() + days);
    return formatDate(date); // Reutiliza a função formatDate
};

/**
 * Pausa a execução.
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Constrói o payload base para as requisições Omie.
 */
const buildOmiePayload = (call, params) => ({
    call,
    param: params,
    app_key: OMIE_APP_KEY,
    app_secret: OMIE_APP_SECRET
});

/**
 * Envia uma requisição POST para a API Omie com retries e tratamento de erros.
 */
const safePost = async (url, payload, msg) => {
    let currentRetry = 0;
    let lastErrorDetails = null; // Variável para armazenar os detalhes do último erro

    while (currentRetry < MAX_RETRIES) {
        try {
            const response = await axios.post(url, payload, { headers: { 'Content-type': 'application/json' }, timeout: 30000 });
            console.log(`[OK] ${msg}`, response.data);
            return { success: true, data: response.data, status_code: response.status };
        } catch (err) { // 'err' é o parâmetro do catch
            // Guarda os detalhes do erro para uso no final, se houver
            const erroData = err.response?.data;
            const httpStatus = err.response?.status;
            const errorMessage = erroData ? JSON.stringify(erroData) : err.message;

            lastErrorDetails = erroData || err.message; // Atualiza com os detalhes do erro atual

            // Tratamento de bloqueio por consumo indevido da API Omie (se Omie impõe um atraso)
            if (erroData?.faultcode === 'MISUSE_API_PROCESS' && erroData.faultstring?.includes('API bloqueada por consumo indevido')) {
                const segundosBloqueio = parseInt(erroData.faultstring.match(/\d+/)?.[0] || "60", 10);
                const waitTime = (segundosBloqueio + 5) * 1000;
                console.warn(`[WARN] API bloqueada. Aguardando ${waitTime / 1000}s... (Tentativa ${++currentRetry}/${MAX_RETRIES})`);
                await sleep(waitTime);
                continue; // Tenta novamente após a espera forçada
            }

            // Tratamento específico para "Lançamento já cadastrado" (SOAP-ENV:Client-102)
            if (erroData?.faultcode === 'SOAP-ENV:Client-102' && erroData?.faultstring?.includes('Lançamento já cadastrado para o Código de Integração')) {
                console.log(`[INFO] ${msg} - Omie informou que o lançamento de integração ${payload.param[0]?.codigo_lancamento_integracao} já existe. Marcado como OK.`);
                return { success: true, data: 'OK (Lançamento já existe na Omie)', status_code: httpStatus || 200 };
            }

            // Tratamento de registro já existente genérico ou código de erro 90001300
            if (erroData?.faultstring?.includes('já existe') || erroData?.codigo_erro === 90001300) {
                console.log(`[INFO] ${msg} - Omie informou que o registro já existe ou código erro 90001300. Marcado como OK.`);
                return { success: true, data: 'OK (Registro existente ou duplicado)', status_code: httpStatus || 200 };
            }

            // Outros erros: loga e tenta novamente com backoff exponencial
            console.error(`[ERRO] ${msg} - Status HTTP: ${httpStatus || 'N/A'}. Detalhes: ${errorMessage}`);
            currentRetry++;
            if (currentRetry < MAX_RETRIES) {
                const delay = BASE_RETRY_DELAY_MS * Math.pow(2, currentRetry - 1);
                console.log(`[RETRY] Tentando novamente em ${delay / 1000}s... (Tentativa ${currentRetry}/${MAX_RETRIES})`);
                await sleep(delay);
            }
        }
    }
    // Usa lastErrorDetails para garantir que a variável exista e seja o último erro registrado
    console.error(`[FALHA FATAL] ${msg} - Falha após ${MAX_RETRIES} tentativas. Último erro: ${JSON.stringify(lastErrorDetails || 'Desconhecido')}`);
    // No retorno final, 'err' não está mais no escopo, então usamos 'lastErrorDetails'
    return { success: false, error: `Falha após ${MAX_RETRIES} tentativas. Último erro: ${JSON.stringify(lastErrorDetails || 'Desconhecido')}`, status_code: 500 };
};

// --- Funções de Conexão e Consulta ao Banco de Dados ---

/**
 * Inicializa e retorna o pool de conexões do banco de dados.
 */
const initializeDbPool = async () => {
    if (!dbPool || !dbPool.connected) {
        dbPool = new sql.ConnectionPool(dbConfig);
        await dbPool.connect();
        console.log('[DB] Conexão com o banco de dados estabelecida.');
    }
    return dbPool;
};

/**
 * Busca registros atualizados da view Vw_Digitacao.
 */
const getDigitacaoAtualizada = async () => {
    try {
        const pool = await initializeDbPool();
        const result = await pool.request().query('SELECT * FROM Vw_Digitacao');
        return result.recordset;
    } catch (err) {
        console.error('[ERRO DB] Erro ao consultar a view Vw_Digitacao:', err.message);
        throw new Error(`Falha ao consultar DB: ${err.message}`);
    }
};

// --- Lógica de Processamento Principal ---

/**
 * Processa um único registro do banco de dados e enfileira as interações com a API Omie.
 */
const processRecord = async (registro, counters) => {
    const codigoIntegracaoBase = registro.contrato_id;
    const baseDate = registro.data_status || registro.data_vencimento || new Date();

    // Data para vencimento/previsão (pode ser futura)
    const dataVencimentoPrevista = addDays(baseDate, 3);
    // Data para liquidação/baixa (DEVE ser atual ou passada)
    const dataBaixa = formatDate(new Date()); // Sempre a data atual

    const valorDocumento = registro.Vlr_Bruto || registro.valor_base_a_vista || registro.valor_base_producao || registro.valor_documento || 0;
    const valorComissao = registro.vlr_cms_total_repasse || registro.vlr_cms_a_vista_repasse || registro.vlr_cms_total_empresa || registro.vlr_cms_a_vista_empresa || registro.Vlr_Liquido || registro.valor_comissao || 0;

    // Gerar um sufixo único (timestamp) para cada tentativa de criação/relançamento
    const uniqueSuffix = new Date().getTime();

    // Códigos de integração únicos para CR e CP
    const codigoIntegracaoCR = `CR_${codigoIntegracaoBase}_${uniqueSuffix}`;
    const codigoIntegracaoCP = `CP_${codigoIntegracaoBase}_${uniqueSuffix}`;

    console.log(`\n--- Processando Contrato ID: ${codigoIntegracaoBase} ---`);
    console.log(`Dados - Vlr Doc: ${valorDocumento}, Vlr Comissão: ${valorComissao}, Data Venc. Prevista: ${dataVencimentoPrevista}, Data Baixa: ${dataBaixa}`);
    console.log(`Códigos de Integração Omie: CR=${codigoIntegracaoCR}, CP=${codigoIntegracaoCP}`); // Para depuração

    // Função interna para enfileirar e processar a chamada Omie
    const queueOmieCall = async (url, payload, msg) => {
        // Enfileira a execução da função safePost. p-queue gerencia o ritmo.
        const res = await omieQueue.add(() => safePost(url, payload, msg));
        res.success ? counters.successfulOmieOperations++ : counters.failedOmieOperations++;
        counters.results.push(res);
        return res; // Retorna o resultado da operação para controle de fluxo
    };

    // --- CONTA A RECEBER ---
    // Primeiro, sempre tentamos INCLUIR a conta a receber se não for cancelado.
    if (registro.status_banco !== "Cancelado") {
        console.log(`[ACAO] Incluindo conta a receber para contrato ${codigoIntegracaoBase} (cód. ${codigoIntegracaoCR})`);
        const payloadIncluirReceber = buildOmiePayload("IncluirContaReceber", [{
            codigo_lancamento_integracao: codigoIntegracaoCR,
            codigo_cliente_fornecedor: CODIGO_FORNECEDOR_FIXO,
            codigo_cliente_fornecedor_integracao: CODIGO_FORNECEDOR_FIXO,
            data_vencimento: dataVencimentoPrevista, // Data de vencimento pode ser futura
            valor_documento: valorDocumento,
            numero_documento: codigoIntegracaoBase,
            codigo_categoria: registro.codigo_categoria || "",
            data_previsao: dataVencimentoPrevista, // Data de previsão pode ser futura
            id_conta_corrente: CODIGO_CONTA_CORRENTE_FIXO
        }]);
        // Capturamos o resultado da inclusão para decidir se tentamos a baixa
        const inclusaoCRResult = await queueOmieCall(`${OMIE_BASE_URL_FINANCAS}contareceber/`, payloadIncluirReceber, `Inclusão CR p/ Contrato ${codigoIntegracaoCR}`);

        // SOMENTE se a inclusão foi bem-sucedida E o status do seu sistema permite a baixa
        if (inclusaoCRResult.success && registro.status_banco === "Recebido do Banco") {
            console.log(`[ACAO] Lançando recebimento (baixa) para contrato ${codigoIntegracaoBase} (cód. ${codigoIntegracaoCR})`);
            const payloadRecebimento = buildOmiePayload("LancarRecebimento", [{
                codigo_lancamento_integracao: codigoIntegracaoCR,
                codigo_lancamento: 0, codigo_baixa: 0,
                codigo_conta_corrente: CODIGO_CONTA_CORRENTE_FIXO,
                valor: valorDocumento,
                data: dataBaixa, // *** USE dataBaixa (data atual) AQUI ***
                observacao: `Recebimento Contrato ID ${codigoIntegracaoBase} (API via ${codigoIntegracaoCR})`
            }]);
            await queueOmieCall(`${OMIE_BASE_URL_FINANCAS}contareceber/`, payloadRecebimento, `Baixa CR p/ Contrato ${codigoIntegracaoCR}`);
        } else if (inclusaoCRResult.success && registro.status_banco !== "Recebido do Banco") {
            console.log(`[INFO] Inclusão CR para ${codigoIntegracaoCR} bem-sucedida, mas status_banco não é "Recebido do Banco". Sem baixa.`);
        } else {
            console.log(`[WARN] Inclusão CR para ${codigoIntegracaoCR} falhou ou não foi processada. Não será tentada a baixa.`);
        }
    } else {
        console.log(`[INFO] Contrato ${codigoIntegracaoBase} com status_banco "Cancelado". Sem ação de Conta a Receber.`);
    }

    // --- CONTA A PAGAR (Comissão) ---
    // Mesma lógica: incluir primeiro, depois baixar se o status permitir.
    if (registro.status_cliente === "Pago ao Cliente" || registro.Status_Fim_Prop === "Pago ao Cliente") {
        console.log(`[ACAO] Incluindo conta a pagar (comissão) para contrato ${codigoIntegracaoBase} (cód. ${codigoIntegracaoCP})`);
        const payloadIncluirPagar = buildOmiePayload("IncluirContaPagar", [{
            codigo_lancamento_integracao: codigoIntegracaoCP,
            codigo_cliente_fornecedor: CODIGO_FORNECEDOR_FIXO,
            codigo_cliente_fornecedor_integracao: CODIGO_FORNECEDOR_FIXO,
            data_vencimento: dataVencimentoPrevista, // Data de vencimento pode ser futura
            valor_documento: valorComissao,
            numero_documento: `CMS-${codigoIntegracaoBase}`,
            codigo_categoria: registro.codigo_categoria_comissao || registro.codigo_categoria || "",
            data_previsao: dataVencimentoPrevista, // Data de previsão pode ser futura
            id_conta_corrente: CODIGO_CONTA_CORRENTE_FIXO
        }]);
        const inclusaoCPResult = await queueOmieCall(`${OMIE_BASE_URL_FINANCAS}contapagar/`, payloadIncluirPagar, `Inclusão CP Comissão p/ Contrato ${codigoIntegracaoCP}`);

        // Somente se a inclusão foi bem-sucedida E o status do seu sistema permite a baixa
        if (inclusaoCPResult.success && (registro.status_comissao === "Comissão Paga" || registro.Status_Fim_Prop === "Comissão Paga")) {
            console.log(`[ACAO] Lançando pagamento (baixa de comissão) para contrato ${codigoIntegracaoBase} (cód. ${codigoIntegracaoCP})`);
            const payloadPagarBaixa = buildOmiePayload("LancarPagamento", [{
                codigo_lancamento_integracao: codigoIntegracaoCP,
                codigo_lancamento: 0, codigo_baixa: 0,
                codigo_conta_corrente: CODIGO_CONTA_CORRENTE_FIXO,
                valor: valorComissao,
                data: dataBaixa, // *** USE dataBaixa (data atual) AQUI ***
                observacao: `Baixa de comissão Contrato ID ${codigoIntegracaoBase} (API via ${codigoIntegracaoCP}).`
            }]);
            await queueOmieCall(`${OMIE_BASE_URL_FINANCAS}contapagar/`, payloadPagarBaixa, `Baixa CP Comissão p/ Contrato ${codigoIntegracaoCP}`);
        } else if (inclusaoCPResult.success && !(registro.status_comissao === "Comissão Paga" || registro.Status_Fim_Prop === "Comissão Paga")) {
            console.log(`[INFO] Inclusão CP para ${codigoIntegracaoCP} bem-sucedida, mas comissão não é "Paga". Sem baixa.`);
        } else {
            console.log(`[WARN] Inclusão CP para ${codigoIntegracaoCP} falhou ou não foi processada. Não será tentada a baixa.`);
        }
    } else {
        console.log(`[INFO] Cliente do contrato ${codigoIntegracaoBase} ainda não "Pago". Sem CP (Comissão).`);
    }
};

/**
 * Orquestra o processamento completo dos registros do banco.
 */
const processDigitacao = async () => {
    const counters = {
        totalRegistrosEncontrados: 0,
        registrosProcessados: 0,
        successfulOmieOperations: 0,
        failedOmieOperations: 0,
        results: []
    };

    try {
        console.log('\n--- [INICIO] Processamento de Integração Omie ---');
        console.log('[INFO] Buscando registros atualizados do banco de dados...');
        const registros = await getDigitacaoAtualizada();
        counters.totalRegistrosEncontrados = registros.length;
        console.log(`[INFO] ${registros.length} registros encontrados para processar.`);

        for (const registro of registros) {
            counters.registrosProcessados++;
            await processRecord(registro, counters);
        }

        // Espera todas as operações na fila terminarem ANTES de fechar a conexão do DB ou retornar o resumo
        console.log('[INFO] Aguardando conclusão de todas as operações enfileiradas na Omie...');
        await omieQueue.onIdle(); // Garante que todas as tarefas na fila foram processadas

        console.log('\n--- [FIM] Resumo do Processamento ---');
        console.log(`Total de Registros Encontrados no Banco: ${counters.totalRegistrosEncontrados}`);
        console.log(`Registros do Banco Processados: ${counters.registrosProcessados}`);
        console.log(`Operações Omie Enviadas com Sucesso (incluindo duplicados): ${counters.successfulOmieOperations}`);
        console.log(`Operações Omie com Falha (após retries): ${counters.failedOmieOperations}`);

        return { status: 'concluido', ...counters };

    } catch (err) {
        console.error('[ERRO GERAL] Falha crítica no processo de digitação:', err);
        return { status: 'erro_critico', error: err.message, ...counters };
    } finally {
        if (dbPool && dbPool.connected) {
            try {
                await dbPool.close();
                console.log('[DB] Conexão com o banco de dados fechada.');
            } catch (e) {
                console.error('[ERRO DB] Erro ao tentar fechar a conexão do banco de dados:', e.message);
            }
        }
    }
};

// --- Rotas da API Express ---

app.post('/api/omie/automatizar', async (req, res) => {
    console.log(`[API] Requisição POST recebida em /api/omie/automatizar em ${new Date().toLocaleString('pt-BR')}`);
    const processingSummary = await processDigitacao();
    res.json(processingSummary);
});

// --- Inicialização do Servidor ---

app.listen(PORT, () => {
    console.log(`API Omie automatizadora rodando na porta ${PORT}`);
    console.log('Certifique-se de que as variáveis de ambiente (DB_USER, DB_PASSWORD, DB_SERVER, DB_DATABASE, OMIE_APP_KEY, OMIE_APP_SECRET) estão configuradas.');

    initializeDbPool().catch(err => {
        console.error(`[CRITICAL] Falha ao conectar ao banco de dados na inicialização: ${err.message}`);
    });
});