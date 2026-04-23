
import { Fragment, useState, useEffect } from "react";
import axios from "axios";

const API_URL = process.env.REACT_APP_API_URL || "http://127.0.0.1:8000";
const DEFAULT_SECTIONS = [
  "Оборудование",
  "Монтажные работы",
  "Пусконаладочные работы",
  "Кабельные линии",
  "Материалы и расходники",
  "Доставка и логистика",
  "Проектирование",
  "Прочее",
];

const emptyMaterial = { name: "", characteristics: "", unit: "", price: "", source: "" };
const emptyItem = {
  section: "Оборудование",
  name: "",
  characteristics: "",
  unit: "",
  quantity: 1,
  unit_price: "",
  source: "",
};
const emptySmetaDetails = {
  parent_id: null,
  name: "",
  customer_name: "",
  customer_details: "",
  contractor_name: "",
  contractor_details: "",
  approver_name: "",
  approver_details: "",
  tax_mode: "none",
  tax_rate: 0,
  section_adjustments: {},
};

function App() {
  const [authToken, setAuthToken] = useState(() => localStorage.getItem("smeta_token") || "");
  const [currentUser, setCurrentUser] = useState(null);
  const [loginForm, setLoginForm] = useState({ email: "", password: "" });
  const [authMode, setAuthMode] = useState("login");
  const [materials, setMaterials] = useState([]);
  const [smetas, setSmetas] = useState([]);
  const [sections, setSections] = useState(DEFAULT_SECTIONS);
  const [selectedSmetaId, setSelectedSmetaId] = useState("");
  const [file, setFile] = useState(null);
  const [importMode, setImportMode] = useState("standard");
  const [supplierUrl, setSupplierUrl] = useState("");
  const [materialQuery, setMaterialQuery] = useState("");
  const [materialType, setMaterialType] = useState("equipment");
  const [technologyFilter, setTechnologyFilter] = useState("");
  const [megapixelsFilter, setMegapixelsFilter] = useState("");
  const [priceToFilter, setPriceToFilter] = useState("");
  const [smetaName, setSmetaName] = useState("");
  const [smetaDetails, setSmetaDetails] = useState(emptySmetaDetails);
  const [shareForm, setShareForm] = useState({ email: "", permission: "view" });
  const [adminUsers, setAdminUsers] = useState([]);
  const [adminAccess, setAdminAccess] = useState([]);
  const [adminUserSmetas, setAdminUserSmetas] = useState([]);
  const [adminSelectedUserId, setAdminSelectedUserId] = useState("");
  const [adminBusy, setAdminBusy] = useState(false);
  const [materialForm, setMaterialForm] = useState(emptyMaterial);
  const [itemForm, setItemForm] = useState(emptyItem);
  const [quantityByMaterial, setQuantityByMaterial] = useState({});
  const [expandedItems, setExpandedItems] = useState({});
  const [aiSettings, setAiSettings] = useState({
    base_url: "https://api.vsegpt.ru/v1",
    model: "",
    has_api_key: false,
    masked_api_key: "",
    assistant_prompt: "",
  });
  const [apiKeyInput, setApiKeyInput] = useState("");
  const [models, setModels] = useState([]);
  const [aiPrompt, setAiPrompt] = useState("");
  const [aiResponse, setAiResponse] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const selectedSmeta = smetas.find(smeta => smeta.id === Number(selectedSmetaId));
  const selectedAdminUser = adminUsers.find(user => String(user.id) === String(adminSelectedUserId));

  useEffect(() => {
    if (authToken) {
      axios.defaults.headers.common.Authorization = `Bearer ${authToken}`;
      localStorage.setItem("smeta_token", authToken);
    } else {
      delete axios.defaults.headers.common.Authorization;
      localStorage.removeItem("smeta_token");
    }
  }, [authToken]);

  const wholeQuantityInput = (value) => {
    const digits = String(value || "").replace(/\D/g, "");
    return digits ? String(Math.max(1, Number(digits))) : "";
  };

  const wholeQuantityValue = (value) => Math.max(1, parseInt(wholeQuantityInput(value) || "1", 10));

  const formatError = (err) => {
    const detail = err.response?.data?.detail;
    if (Array.isArray(detail)) {
      return detail.map(item => item.msg || JSON.stringify(item)).join("; ");
    }
    if (detail && typeof detail === "object") {
      return detail.message || JSON.stringify(detail);
    }
    return detail || "Не удалось выполнить действие";
  };

  useEffect(() => {
    if (!authToken) {
      return;
    }
    refreshData();
    loadAiSettings();
    loadSections();
    axios.get(`${API_URL}/auth/me`)
      .then(res => setCurrentUser(res.data))
      .catch(() => {
        setAuthToken("");
        setCurrentUser(null);
      });
  }, [authToken]);

  useEffect(() => {
    if (!authToken || !currentUser?.is_admin) {
      setAdminUsers([]);
      setAdminAccess([]);
      setAdminUserSmetas([]);
      setAdminSelectedUserId("");
      return;
    }
    loadAdminData();
  }, [authToken, currentUser?.is_admin, selectedSmetaId]);

  useEffect(() => {
    if (authToken && currentUser?.is_admin) {
      loadAiSettings();
    }
  }, [authToken, currentUser?.is_admin]);

  useEffect(() => {
    if (!authToken || !selectedSmetaId) {
      return;
    }
  }, [authToken, selectedSmetaId]);

  useEffect(() => {
    if (!authToken) {
      return undefined;
    }
    const timer = setTimeout(() => {
      loadMaterials(materialQuery, materialType);
    }, 220);
    return () => clearTimeout(timer);
  }, [authToken, materialQuery, materialType, technologyFilter, megapixelsFilter, priceToFilter]);

  useEffect(() => {
    if (selectedSmeta) {
      setSmetaDetails({
        name: selectedSmeta.name || "",
        parent_id: selectedSmeta.parent_id || null,
        customer_name: selectedSmeta.customer_name || "",
        customer_details: selectedSmeta.customer_details || "",
        contractor_name: selectedSmeta.contractor_name || "",
        contractor_details: selectedSmeta.contractor_details || "",
        approver_name: selectedSmeta.approver_name || "",
        approver_details: selectedSmeta.approver_details || "",
        tax_mode: selectedSmeta.tax_mode || "none",
        tax_rate: selectedSmeta.tax_rate || 0,
        section_adjustments: selectedSmeta.section_adjustments || {},
      });
    } else {
      setSmetaDetails(emptySmetaDetails);
    }
  }, [selectedSmetaId]);

  const refreshData = async () => {
    const [smetasRes] = await Promise.all([
      axios.get(`${API_URL}/smetas`),
      loadMaterials(materialQuery, materialType),
    ]);
    setSmetas(smetasRes.data);
    if (!selectedSmetaId && smetasRes.data.length > 0) {
      setSelectedSmetaId(String(smetasRes.data[0].id));
    }
  };

  const handleLogin = async () => {
    setError("");
    setMessage("");
    try {
      const res = await axios.post(`${API_URL}/auth/login`, loginForm);
      setAuthToken(res.data.access_token);
      setCurrentUser(res.data.user);
      setLoginForm({ email: "", password: "" });
    } catch (err) {
      setError(formatError(err));
    }
  };

  const handleRegister = async () => {
    setError("");
    setMessage("");
    try {
      const res = await axios.post(`${API_URL}/auth/register`, loginForm);
      setAuthToken(res.data.access_token);
      setCurrentUser(res.data.user);
      setLoginForm({ email: "", password: "" });
    } catch (err) {
      setError(formatError(err));
    }
  };

  const handleLogout = () => {
    setAuthToken("");
    setCurrentUser(null);
    setSmetas([]);
    setSelectedSmetaId("");
    setAdminUsers([]);
    setAdminAccess([]);
  };

  const loadMaterials = async (q = materialQuery, type = materialType) => {
    const res = await axios.get(`${API_URL}/materials`, {
      params: {
        q,
        item_type: type,
        technology: technologyFilter,
        megapixels: megapixelsFilter,
        price_to: priceToFilter || undefined,
        limit: 200,
      },
    });
    setMaterials(res.data);
    return res.data;
  };

  const loadSections = async () => {
    try {
      const res = await axios.get(`${API_URL}/sections`);
      setSections(res.data.sections || DEFAULT_SECTIONS);
    } catch (err) {
      setSections(DEFAULT_SECTIONS);
    }
  };

  const runAction = async (action, successText) => {
    setError("");
    setMessage("");
    try {
      const actionMessage = await action();
      setMessage(actionMessage || successText);
    } catch (err) {
      setError(formatError(err));
    }
  };

  const loadAiSettings = async () => {
    if (!currentUser?.is_admin) {
      return;
    }
    try {
      const res = await axios.get(`${API_URL}/settings/ai`);
      setAiSettings(res.data);
    } catch (err) {
      setError("Не удалось загрузить настройки AI");
    }
  };

  const loadAdminData = async () => {
    if (!currentUser?.is_admin) {
      return;
    }
    try {
      const [usersRes, accessRes] = await Promise.all([
        axios.get(`${API_URL}/admin/users`),
        selectedSmeta
          ? axios.get(`${API_URL}/admin/smetas/${selectedSmeta.id}/access`)
          : Promise.resolve({ data: { access: [] } }),
      ]);
      const users = usersRes.data || [];
      setAdminUsers(users);
      const selectedId = adminSelectedUserId && users.some(user => String(user.id) === String(adminSelectedUserId))
        ? adminSelectedUserId
        : (users[0] ? String(users[0].id) : "");
      if (selectedId !== adminSelectedUserId) {
        setAdminSelectedUserId(selectedId);
      }
      setAdminAccess(accessRes.data?.access || []);
      if (selectedId) {
        const userSmetasRes = await axios.get(`${API_URL}/admin/users/${selectedId}/smetas`);
        setAdminUserSmetas(userSmetasRes.data?.smetas || []);
      } else {
        setAdminUserSmetas([]);
      }
    } catch (err) {
      setError(formatError(err));
    }
  };

  const loadAdminUserSmetas = async (userId) => {
    if (!currentUser?.is_admin || !userId) {
      setAdminUserSmetas([]);
      return;
    }
    try {
      const res = await axios.get(`${API_URL}/admin/users/${userId}/smetas`);
      setAdminUserSmetas(res.data?.smetas || []);
    } catch (err) {
      setAdminUserSmetas([]);
      setError(formatError(err));
    }
  };

  const handleSaveAiSettings = async () => {
    await runAction(async () => {
      const res = await axios.post(`${API_URL}/settings/ai`, {
        base_url: aiSettings.base_url,
        api_key: apiKeyInput,
        model: aiSettings.model,
        assistant_prompt: aiSettings.assistant_prompt || "",
      });
      setAiSettings(res.data);
      setApiKeyInput("");
    }, "Настройки AI сохранены");
  };

  const handleLoadModels = async () => {
    await runAction(async () => {
      const res = await axios.get(`${API_URL}/settings/ai/models`);
      setModels(res.data.models);
      if (!aiSettings.model && res.data.models.length > 0) {
        setAiSettings(current => ({ ...current, model: res.data.models[0].id }));
      }
    }, "Список моделей загружен");
  };

  const handleSelectModel = async (modelId) => {
    setAiSettings(current => ({ ...current, model: modelId }));
    await runAction(async () => {
      const res = await axios.post(`${API_URL}/settings/ai`, {
        base_url: aiSettings.base_url,
        api_key: apiKeyInput,
        model: modelId,
      });
      setAiSettings(res.data);
    }, "Модель выбрана");
  };

  const handleUpload = async () => {
    if (!file && (importMode === "standard" || !supplierUrl.trim())) {
      setError("Выберите файл или укажите URL поставщика");
      return;
    }
    await runAction(async () => {
      const formData = new FormData();
      if (file) {
        formData.append("file", file);
      }
      let res;
      if (importMode === "ai") {
        formData.append("url", supplierUrl);
        res = await axios.post(`${API_URL}/materials/import-ai`, formData);
      } else {
        res = await axios.post(`${API_URL}/materials/import`, formData);
      }
      await refreshData();
      setFile(null);
      setSupplierUrl("");
      return importMode === "ai"
        ? `AI импортировал: ${res.data.imported}, пропущено: ${res.data.skipped}`
        : `Импортировано строк: ${res.data.imported}`;
    }, "Материалы импортированы");
  };

  const handleCreateSmeta = async () => {
    if (!smetaName.trim()) {
      setError("Введите название сметы");
      return;
    }
    await runAction(async () => {
      const res = await axios.post(`${API_URL}/smetas`, { name: smetaName });
      setSmetas(current => [res.data, ...current]);
      setSelectedSmetaId(String(res.data.id));
      setSmetaName("");
    }, "Смета создана");
  };

  const handleCreateMaterial = async () => {
    if (!materialForm.name.trim() || materialForm.price === "") {
      setError("Заполните название и цену материала");
      return;
    }
    await runAction(async () => {
      await axios.post(`${API_URL}/materials`, {
        ...materialForm,
        item_type: materialType === "work" ? "work" : "equipment",
        price: Number(materialForm.price),
      });
      setMaterialForm(emptyMaterial);
      await refreshData();
    }, "Материал добавлен");
  };

  const updateSelectedSmeta = (updatedSmeta) => {
    setSmetas(current => current.map(smeta => smeta.id === updatedSmeta.id ? updatedSmeta : smeta));
  };

  const normalizeSmetaForSave = (details) => ({
    ...details,
    parent_id: details.parent_id || null,
    tax_rate: Number(details.tax_rate || 0),
    section_adjustments: Object.fromEntries(
      Object.entries(details.section_adjustments || {}).map(([section, percent]) => [
        section,
        Number(String(percent || 0).replace(",", ".")) || 0,
      ])
    ),
  });

  const calculatePreviewSmeta = (smeta, details) => {
    if (!smeta) {
      return { subtotal: 0, tax_amount: 0, total: 0, items: [] };
    }
    const adjustments = details.section_adjustments || {};
    const items = (smeta.items || []).map(item => {
      const section = item.section || "Оборудование";
      const percent = Number(String(adjustments[section] ?? 0).replace(",", ".")) || 0;
      const effectiveUnitPrice = Math.round((item.unit_price || 0) * (1 + percent / 100) * 100) / 100;
      const total = Math.round((item.quantity || 0) * effectiveUnitPrice * 100) / 100;
      return {
        ...item,
        effective_unit_price: effectiveUnitPrice,
        section_adjustment_percent: percent,
        total,
      };
    });
    const subtotal = Math.round(items.reduce((sum, item) => sum + item.total, 0) * 100) / 100;
    const taxRate = Number(details.tax_rate || 0);
    let taxAmount = 0;
    let total = subtotal;
    if (details.tax_mode === "vat_added" && taxRate > 0) {
      taxAmount = Math.round(subtotal * taxRate) / 100;
      total = Math.round((subtotal + taxAmount) * 100) / 100;
    } else if (details.tax_mode === "vat_included" && taxRate > 0) {
      taxAmount = Math.round((subtotal * taxRate / (100 + taxRate)) * 100) / 100;
    }
    return { ...smeta, ...details, items, subtotal, tax_amount: taxAmount, total };
  };

  const handleDeleteSmeta = async () => {
    if (!selectedSmeta) {
      setError("Смета не выбрана");
      return;
    }
    await runAction(async () => {
      await axios.delete(`${API_URL}/smetas/${selectedSmeta.id}`);
      const next = smetas.filter(smeta => smeta.id !== selectedSmeta.id);
      setSmetas(next);
      setSelectedSmetaId(next.length > 0 ? String(next[0].id) : "");
    }, "Смета удалена");
  };

  const handleBranchSmeta = async () => {
    if (!selectedSmeta) {
      setError("Смета не выбрана");
      return;
    }
    await runAction(async () => {
      const res = await axios.post(`${API_URL}/smetas/${selectedSmeta.id}/branch`);
      setSmetas(current => [...current, res.data]);
      setSelectedSmetaId(String(res.data.id));
      return `Создана ветка «${res.data.name}»`;
    }, "Ветка сметы создана");
  };

  const updateSmetaDetails = (field, value) => {
    setSmetaDetails(current => ({ ...current, [field]: value }));
  };

  const updateSectionAdjustment = (section, value) => {
    const normalized = String(value || "").replace(",", ".");
    setSmetaDetails(current => ({
      ...current,
      section_adjustments: {
        ...(current.section_adjustments || {}),
        [section]: normalized,
      },
    }));
  };

  const handleSaveSmetaDetails = async () => {
    if (!selectedSmeta) {
      setError("Смета не выбрана");
      return;
    }
    if (!smetaDetails.name.trim()) {
      setError("Введите название сметы");
      return;
    }
    await runAction(async () => {
      const res = await axios.patch(`${API_URL}/smetas/${selectedSmeta.id}`, normalizeSmetaForSave(smetaDetails));
      updateSelectedSmeta(res.data);
    }, "Реквизиты сметы сохранены");
  };

  const handleShareSmeta = async () => {
    if (!selectedSmeta) {
      setError("Смета не выбрана");
      return;
    }
    if (!shareForm.email.trim()) {
      setError("Введите email пользователя");
      return;
    }
    await runAction(async () => {
      const res = await axios.post(`${API_URL}/smetas/${selectedSmeta.id}/share`, shareForm);
      setShareForm({ email: "", permission: "view" });
      await loadAdminData();
      return `Доступ для ${res.data.email}: ${res.data.permission === "edit" ? "редактирование" : "просмотр"}`;
    }, "Доступ открыт");
  };

  const handleToggleAdmin = async (targetUser) => {
    if (!currentUser?.is_admin || adminBusy) {
      return;
    }
    setAdminBusy(true);
    await runAction(async () => {
      const res = await axios.patch(`${API_URL}/admin/users/${targetUser.id}`, {
        is_admin: !targetUser.is_admin,
      });
      setAdminUsers(current => current.map(user => (user.id === res.data.id ? res.data : user)));
      if (targetUser.id === currentUser.id) {
        setCurrentUser(res.data);
      }
      await loadAdminData();
      return `${res.data.email}: ${res.data.is_admin ? "админ" : "обычный пользователь"}`;
    }, "Права пользователя обновлены");
    setAdminBusy(false);
  };

  const handleSelectAdminUser = async (userId) => {
    const normalizedId = String(userId || "");
    setAdminSelectedUserId(normalizedId);
    await loadAdminUserSmetas(normalizedId);
  };

  const handleDeleteUser = async (targetUser) => {
    if (!currentUser?.is_admin || adminBusy) {
      return;
    }
    if (!window.confirm(`Удалить пользователя ${targetUser.email}?`)) {
      return;
    }
    setAdminBusy(true);
    await runAction(async () => {
      await axios.delete(`${API_URL}/admin/users/${targetUser.id}`);
      if (targetUser.id === currentUser.id) {
        handleLogout();
      } else {
        if (String(adminSelectedUserId) === String(targetUser.id)) {
          setAdminSelectedUserId("");
          setAdminUserSmetas([]);
        }
        await loadAdminData();
      }
      return `Пользователь ${targetUser.email} удалён`;
    }, "Пользователь удалён");
    setAdminBusy(false);
  };

  const handleRevokeAccess = async (userId) => {
    if (!selectedSmeta || !currentUser?.is_admin || adminBusy) {
      return;
    }
    setAdminBusy(true);
    await runAction(async () => {
      await axios.delete(`${API_URL}/admin/smetas/${selectedSmeta.id}/access/${userId}`);
      await loadAdminData();
      return "Доступ отозван";
    }, "Доступ отозван");
    setAdminBusy(false);
  };

  const handleExportExcel = () => {
    if (!selectedSmeta) {
      setError("Смета не выбрана");
      return;
    }
    window.location.href = `${API_URL}/smetas/${selectedSmeta.id}/export.xlsx?token=${encodeURIComponent(authToken)}`;
  };

  const handlePrintSmeta = () => {
    if (!selectedSmeta) {
      setError("Смета не выбрана");
      return;
    }
    window.open(`${API_URL}/smetas/${selectedSmeta.id}/print?token=${encodeURIComponent(authToken)}`, "_blank", "noopener,noreferrer");
  };

  const handleCheckSmeta = async () => {
    if (!selectedSmeta) {
      setError("Смета не выбрана");
      return;
    }
    await runAction(async () => {
      const res = await axios.post(`${API_URL}/smetas/${selectedSmeta.id}/check`);
      updateSelectedSmeta(res.data.smeta);
      const parts = [
        ...(res.data.results || []),
        ...(res.data.issues || []).map(issue => `Проверить: ${issue}`),
      ];
      setAiResponse(parts.join("\n"));
      return parts.length ? parts.join("\n") : "Смета проверена, замечаний нет";
    }, "Смета проверена");
  };

  const handleAddMaterialToSmeta = async (material) => {
    if (!selectedSmeta) {
      setError("Сначала создайте или выберите смету");
      return;
    }
    const quantity = wholeQuantityValue(quantityByMaterial[material.id]);
    await runAction(async () => {
      const res = await axios.post(
        `${API_URL}/smetas/${selectedSmeta.id}/items`,
        {
          name: material.name,
          characteristics: material.characteristics,
          section: material.item_type === "work" ? "Монтажные работы" : "Оборудование",
          unit: material.unit,
          quantity,
          unit_price: material.price,
          source: material.source,
        },
        { params: { material_id: material.id } }
      );
      updateSelectedSmeta(res.data);
    }, "Позиция добавлена в смету");
  };

  const handleAddCustomItem = async () => {
    if (!selectedSmeta) {
      setError("Сначала создайте или выберите смету");
      return;
    }
    if (!itemForm.name.trim() || itemForm.unit_price === "") {
      setError("Заполните название и цену позиции");
      return;
    }
    await runAction(async () => {
      const res = await axios.post(`${API_URL}/smetas/${selectedSmeta.id}/items`, {
        ...itemForm,
        quantity: wholeQuantityValue(itemForm.quantity),
        unit_price: Number(itemForm.unit_price),
      });
      updateSelectedSmeta(res.data);
      setItemForm(emptyItem);
    }, "Позиция добавлена");
  };

  const handleDeleteItem = async (itemId) => {
    await runAction(async () => {
      const res = await axios.delete(`${API_URL}/smetas/${selectedSmeta.id}/items/${itemId}`);
      updateSelectedSmeta(res.data);
    }, "Позиция удалена");
  };

  const handleUpdateItemNumber = async (item, field, value) => {
    if (value === "") {
      return;
    }
    const numberValue = field === "quantity" ? wholeQuantityValue(value) : Number(value);
    if (!selectedSmeta || Number.isNaN(numberValue) || numberValue < 0) {
      return;
    }
    const payload = {
      item_type: item.item_type || "manual",
      section: item.section || "Прочее",
      name: item.name,
      characteristics: item.characteristics || "",
      unit: item.unit || "",
      quantity: field === "quantity" ? Math.max(1, numberValue) : item.quantity,
      unit_price: field === "unit_price" ? numberValue : item.unit_price,
      source: item.source || "",
    };
    await runAction(async () => {
      const res = await axios.patch(`${API_URL}/smetas/${selectedSmeta.id}/items/${item.id}`, payload);
      updateSelectedSmeta(res.data);
      return "Позиция обновлена";
    }, "Позиция обновлена");
  };

  const handleAiRequest = async () => {
    if (!aiPrompt.trim()) {
      setError("Введите запрос для ассистента");
      return;
    }
    await runAction(async () => {
      const res = await axios.post(`${API_URL}/ai/command`, {
        prompt: aiPrompt,
        smeta_id: selectedSmeta?.id || null,
      });
      setAiResponse([res.data.reply, ...(res.data.results || [])].join("\n"));
      setSmetas(res.data.smetas || []);
      if (res.data.selected_smeta_id) {
        setSelectedSmetaId(String(res.data.selected_smeta_id));
      }
    }, "Ассистент выполнил команду");
  };

  const money = (value) => new Intl.NumberFormat("ru-RU", {
    style: "currency",
    currency: "RUB",
    maximumFractionDigits: 2,
  }).format(value || 0);

  const updateMaterialForm = (field, value) => {
    setMaterialForm(current => ({ ...current, [field]: value }));
  };

  const updateItemForm = (field, value) => {
    setItemForm(current => ({ ...current, [field]: value }));
  };

  const toggleItem = (itemId) => {
    setExpandedItems(current => ({ ...current, [itemId]: !current[itemId] }));
  };

  const compactDetails = (text) => {
    const lines = String(text || "")
      .split(/\n|;|\. /)
      .map(line => line.trim())
      .filter(Boolean);
    return lines.slice(0, 1);
  };

  const hasLongDetails = (item) => {
    const text = String(item.characteristics || "");
    return text.length > 90 || text.includes("\n") || text.includes(";") || text.includes(". ");
  };

  const modelPrice = (model) => {
    if (model.input_price == null && model.output_price == null) {
      return "стоимость не указана";
    }
    const input = model.input_price == null ? "?" : model.input_price;
    const output = model.output_price == null ? "?" : model.output_price;
    return `запрос: ${input} · ответ: ${output}`;
  };

  const previewSmeta = calculatePreviewSmeta(selectedSmeta, smetaDetails);
  const parentIdOf = (smeta) => Number(smeta?.parent_id || 0) || null;

  const groupedItems = sections.map(section => ({
    section,
    items: previewSmeta?.items.filter(item => (item.section || "Оборудование") === section) || [],
  })).filter(group => group.items.length > 0 || ["Оборудование", "Монтажные работы", "Пусконаладочные работы"].includes(group.section));

  const childrenByParent = smetas.reduce((acc, smeta) => {
    const parentId = parentIdOf(smeta);
    if (!parentId) {
      return acc;
    }
    return {
      ...acc,
      [parentId]: [...(acc[parentId] || []), smeta],
    };
  }, {});
  const roots = smetas
    .filter(smeta => !parentIdOf(smeta) || !smetas.some(parent => parent.id === parentIdOf(smeta)))
    .sort((a, b) => b.id - a.id);
  const walkSmetaTree = (node, depth = 0, visited = new Set()) => {
    if (visited.has(node.id)) {
      return [];
    }
    const nextVisited = new Set(visited);
    nextVisited.add(node.id);
    const children = (childrenByParent[node.id] || []).sort((a, b) => b.id - a.id);
    return [
      { smeta: node, depth },
      ...children.flatMap(child => walkSmetaTree(child, depth + 1, nextVisited)),
    ];
  };
  const smetaTree = roots.flatMap(root => walkSmetaTree(root));

  if (!authToken) {
    return (
      <main className="login-shell">
        <section className="panel login-panel">
          <p className="eyebrow">Сметный рабочий стол</p>
          <h1>Вход</h1>
          {(message || error) && (
            <div className={error ? "notice error" : "notice"}>
              {error || message}
            </div>
          )}
          <input
            type="email"
            placeholder="Email"
            value={loginForm.email}
            onChange={e => setLoginForm(current => ({ ...current, email: e.target.value }))}
          />
          <input
            type="password"
            placeholder="Пароль"
            value={loginForm.password}
            onChange={e => setLoginForm(current => ({ ...current, password: e.target.value }))}
            onKeyDown={e => {
              if (e.key === "Enter") {
                handleLogin();
              }
            }}
          />
          <button onClick={authMode === "login" ? handleLogin : handleRegister}>
            {authMode === "login" ? "Войти" : "Зарегистрироваться"}
          </button>
          <button className="ghost" onClick={() => setAuthMode(current => current === "login" ? "register" : "login")}>
            {authMode === "login" ? "Нужна регистрация" : "Уже есть аккаунт"}
          </button>
          {authMode === "register" && (
            <p className="muted">
              После регистрации вы сразу войдёте в систему. Админский email недоступен для регистрации.
            </p>
          )}
        </section>
      </main>
    );
  }

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Сметный рабочий стол</p>
          <h1>Материалы, сметы и быстрые проверки</h1>
          {currentUser && <p className="user-line">{currentUser.email}{currentUser.is_admin ? " · админ" : ""}</p>}
        </div>
        <div className="total-box">
          <span>Итого по смете</span>
          <strong>{money(previewSmeta?.total || 0)}</strong>
          <button className="ghost" onClick={handleLogout}>Выйти</button>
        </div>
      </header>

      {(message || error) && (
        <div className={error ? "notice error" : "notice"}>
          {error || message}
        </div>
      )}

      <section className="workspace">
        <aside className="panel sidebar">
          <h2>Сметы</h2>
          <div className="inline-form">
            <input
              type="text"
              placeholder="Новая смета"
              value={smetaName}
              onChange={e => setSmetaName(e.target.value)}
            />
            <button onClick={handleCreateSmeta}>Создать</button>
          </div>

          <div className="smeta-list">
            {smetaTree.map(({ smeta, depth }) => (
              <button
                key={smeta.id}
                className={`${smeta.id === Number(selectedSmetaId) ? "smeta-card active" : "smeta-card"} ${depth > 0 ? "branch" : ""}`}
                style={{ marginLeft: depth ? `${Math.min(depth, 5) * 22}px` : undefined }}
                onClick={() => setSelectedSmetaId(String(smeta.id))}
              >
                <span>{depth > 0 ? `↳ ${smeta.name}` : smeta.name}</span>
                {parentIdOf(smeta) && <em>Ветка от сметы #{parentIdOf(smeta)}</em>}
                <strong>{money(smeta.total)}</strong>
              </button>
            ))}
            {smetas.length === 0 && <p className="muted">Создайте первую смету.</p>}
          </div>
        </aside>

        <section className="panel estimate-panel">
          <div className="section-title">
            <div>
              <h2>{selectedSmeta?.name || "Смета не выбрана"}</h2>
              <p>{selectedSmeta ? `${selectedSmeta.items.length} позиций` : "Выберите смету слева"}</p>
            </div>
            <div className="title-actions">
              <strong>{money(previewSmeta?.total || 0)}</strong>
              {selectedSmeta && (
                <>
                  <button className="ghost" onClick={handleBranchSmeta}>Сделать ветку</button>
                  <button className="ghost" onClick={handleCheckSmeta}>Проверить смету</button>
                  <button className="ghost" onClick={handleExportExcel}>Excel</button>
                  <button className="ghost" onClick={handlePrintSmeta}>PDF</button>
                  <button className="ghost danger" onClick={handleDeleteSmeta}>Удалить смету</button>
                </>
              )}
            </div>
          </div>

          {selectedSmeta && (
            <div className="smeta-details">
              <input
                type="text"
                placeholder="Название сметы"
                value={smetaDetails.name}
                onChange={e => updateSmetaDetails("name", e.target.value)}
              />
              <input
                type="text"
                placeholder="Заказчик"
                value={smetaDetails.customer_name}
                onChange={e => updateSmetaDetails("customer_name", e.target.value)}
              />
              <textarea
                placeholder="Реквизиты заказчика"
                value={smetaDetails.customer_details}
                onChange={e => updateSmetaDetails("customer_details", e.target.value)}
              />
              <input
                type="text"
                placeholder="Исполнитель"
                value={smetaDetails.contractor_name}
                onChange={e => updateSmetaDetails("contractor_name", e.target.value)}
              />
              <textarea
                placeholder="Реквизиты исполнителя"
                value={smetaDetails.contractor_details}
                onChange={e => updateSmetaDetails("contractor_details", e.target.value)}
              />
              <input
                type="text"
                placeholder="Согласующий"
                value={smetaDetails.approver_name}
                onChange={e => updateSmetaDetails("approver_name", e.target.value)}
              />
              <textarea
                placeholder="Реквизиты согласующего"
                value={smetaDetails.approver_details}
                onChange={e => updateSmetaDetails("approver_details", e.target.value)}
              />
              <select
                value={smetaDetails.tax_mode}
                onChange={e => updateSmetaDetails("tax_mode", e.target.value)}
              >
                <option value="none">Без НДС</option>
                <option value="vat_added">НДС сверху</option>
                <option value="vat_included">НДС в том числе</option>
              </select>
              <select
                value={smetaDetails.tax_rate}
                onChange={e => updateSmetaDetails("tax_rate", e.target.value)}
              >
                <option value="0">0%</option>
                <option value="5">5% УСН</option>
                <option value="7">7% УСН</option>
                <option value="10">10%</option>
                <option value="22">22% НДС</option>
              </select>
              <div className="tax-summary">
                <span>До налога: {money(previewSmeta.subtotal || 0)}</span>
                <span>Налог: {money(previewSmeta.tax_amount || 0)}</span>
              </div>
              <button onClick={handleSaveSmetaDetails}>Сохранить реквизиты</button>
              <input
                type="email"
                placeholder="Email для доступа"
                value={shareForm.email}
                onChange={e => setShareForm(current => ({ ...current, email: e.target.value }))}
              />
              <select
                value={shareForm.permission}
                onChange={e => setShareForm(current => ({ ...current, permission: e.target.value }))}
              >
                <option value="view">Только просмотр</option>
                <option value="edit">Совместное редактирование</option>
              </select>
              <button className="ghost" onClick={handleShareSmeta}>Поделиться</button>
            </div>
          )}

          {selectedSmeta && currentUser?.is_admin && (
            <div className="revision-panel">
              <div className="section-title compact">
                <div>
                  <h2>Доступ к смете</h2>
                  <p>{selectedSmeta.name}</p>
                </div>
              </div>
              {adminAccess.length > 0 ? (
                <>
                  <p className="muted">К этой смете есть доступ у {adminAccess.length} пользователей.</p>
                  <div className="admin-access-list">
                    {adminAccess.map(access => (
                      <div key={access.id} className="admin-access-row">
                        <div>
                          <strong>{access.email}</strong>
                          <span>{access.permission === "edit" ? "редактирование" : "просмотр"}</span>
                        </div>
                        <button className="ghost danger" disabled={adminBusy} onClick={() => handleRevokeAccess(access.user_id)}>
                          Отозвать
                        </button>
                      </div>
                    ))}
                  </div>
                </>
              ) : (
                <p className="muted">У этой сметы пока нет расшаренных доступов.</p>
              )}
            </div>
          )}

          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Позиция</th>
                  <th>Кол-во</th>
                  <th>Цена</th>
                  <th>Сумма</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {groupedItems.map(group => (
                  <Fragment key={group.section}>
                    <tr className="section-row">
                      <td colSpan="5">
                        <strong>{group.section}</strong>
                        <label className="section-percent">
                          <input
                            type="number"
                            step="1"
                            value={(smetaDetails.section_adjustments || {})[group.section] ?? 0}
                            onChange={e => updateSectionAdjustment(group.section, e.target.value)}
                          />
                          %
                        </label>
                        <span>{money(group.items.reduce((sum, item) => sum + item.total, 0))}</span>
                      </td>
                    </tr>
                    {group.items.map(item => (
                      <tr key={item.id}>
                        <td>
                          <div className="item-cardline">
                            <strong>{item.name}</strong>
                            {hasLongDetails(item) && (
                              <button className="icon-button" onClick={() => toggleItem(item.id)}>
                                {expandedItems[item.id] ? "Свернуть" : "Детали"}
                              </button>
                            )}
                          </div>
                          <div className={expandedItems[item.id] ? "item-details expanded" : "item-details"}>
                            {!expandedItems[item.id] && compactDetails(item.characteristics).map((line, index) => (
                              <span key={index}>{line}</span>
                            ))}
                            {expandedItems[item.id] && <small>{item.characteristics || "Описание не заполнено"}</small>}
                            <em>{item.unit || "ед."} · {item.source || "без источника"}</em>
                            {item.section_adjustment_percent !== 0 && (
                              <em>Цена с корректировкой раздела: {money(item.effective_unit_price)}</em>
                            )}
                          </div>
                        </td>
                        <td>
                          <input
                            className="table-number"
                            type="number"
                            min="1"
                            step="1"
                            value={Math.round(item.quantity)}
                            onChange={e => handleUpdateItemNumber(item, "quantity", e.target.value)}
                          />
                        </td>
                        <td>
                          <input
                            className="table-number price"
                            type="number"
                            min="0"
                            step="0.01"
                            value={item.unit_price}
                            onChange={e => handleUpdateItemNumber(item, "unit_price", e.target.value)}
                          />
                        </td>
                        <td>{money(item.total)}</td>
                        <td>
                          <button className="ghost danger" onClick={() => handleDeleteItem(item.id)}>
                            Удалить
                          </button>
                        </td>
                      </tr>
                    ))}
                  </Fragment>
                ))}
                {(!selectedSmeta || selectedSmeta.items.length === 0) && (
                  <tr>
                    <td colSpan="5" className="empty">Добавьте материалы или ручную позицию.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          <div className="custom-item">
            <select
              value={itemForm.section}
              onChange={e => updateItemForm("section", e.target.value)}
            >
              {sections.map(section => <option key={section} value={section}>{section}</option>)}
            </select>
            <input
              type="text"
              placeholder="Ручная позиция"
              value={itemForm.name}
              onChange={e => updateItemForm("name", e.target.value)}
            />
            <input
              type="text"
              placeholder="Характеристики"
              value={itemForm.characteristics}
              onChange={e => updateItemForm("characteristics", e.target.value)}
            />
            <input
              type="text"
              placeholder="Ед."
              value={itemForm.unit}
              onChange={e => updateItemForm("unit", e.target.value)}
            />
            <input
              type="number"
              min="1"
              step="1"
              inputMode="numeric"
              pattern="[0-9]*"
              placeholder="Кол-во"
              value={itemForm.quantity}
              onChange={e => updateItemForm("quantity", wholeQuantityInput(e.target.value))}
            />
            <input
              type="number"
              min="0"
              step="0.01"
              placeholder="Цена"
              value={itemForm.unit_price}
              onChange={e => updateItemForm("unit_price", e.target.value)}
            />
            <button onClick={handleAddCustomItem}>Добавить</button>
          </div>
        </section>
      </section>

      <section className="lower-grid">
        <section className="panel">
          <div className="section-title">
            <div>
              <h2>Материалы</h2>
              <p>{materials.length} релевантных позиций</p>
            </div>
          </div>

          <div className="import-box">
            <div className="segmented">
              <button
                className={importMode === "standard" ? "active" : ""}
                onClick={() => setImportMode("standard")}
              >
                Excel как таблица
              </button>
              <button
                className={importMode === "ai" ? "active" : ""}
                onClick={() => setImportMode("ai")}
              >
                AI: Excel/PDF/сайт
              </button>
            </div>
            <input
              type="file"
              accept={importMode === "ai" ? ".xlsx,.xls,.pdf" : ".xlsx,.xls"}
              onChange={e => setFile(e.target.files[0] || null)}
            />
            {importMode === "ai" && (
              <input
                type="url"
                placeholder="Или URL сайта поставщика"
                value={supplierUrl}
                onChange={e => setSupplierUrl(e.target.value)}
              />
            )}
            <button onClick={handleUpload}>
              {importMode === "ai" ? "Распарсить и добавить" : "Импорт Excel"}
            </button>
          </div>

          <div className="catalog-tools">
            <div className="segmented">
              <button
                className={materialType === "equipment" ? "active" : ""}
                onClick={() => setMaterialType("equipment")}
              >
                Оборудование
              </button>
              <button
                className={materialType === "work" ? "active" : ""}
                onClick={() => setMaterialType("work")}
              >
                Работы
              </button>
              <button
                className={materialType === "all" ? "active" : ""}
                onClick={() => setMaterialType("all")}
              >
                Всё
              </button>
            </div>
            <input
              type="search"
              placeholder="Поиск: камера, кабель, монтаж, Optimus..."
              value={materialQuery}
              onChange={e => setMaterialQuery(e.target.value)}
            />
          </div>

          <div className="smart-filters">
            <select value={technologyFilter} onChange={e => setTechnologyFilter(e.target.value)}>
              <option value="">Любая технология</option>
              <option value="ip">IP</option>
              <option value="ahd">AHD</option>
              <option value="poe">PoE</option>
            </select>
            <select value={megapixelsFilter} onChange={e => setMegapixelsFilter(e.target.value)}>
              <option value="">Любое разрешение</option>
              <option value="2">2 Мп</option>
              <option value="4">4 Мп</option>
              <option value="5">5 Мп</option>
              <option value="8">8 Мп</option>
            </select>
            <input
              type="number"
              min="0"
              step="100"
              placeholder="Цена до"
              value={priceToFilter}
              onChange={e => setPriceToFilter(e.target.value)}
            />
            <button
              className="ghost"
              onClick={() => {
                setTechnologyFilter("");
                setMegapixelsFilter("");
                setPriceToFilter("");
              }}
            >
              Сбросить
            </button>
          </div>

          <div className="material-form">
            <input
              type="text"
              placeholder="Название"
              value={materialForm.name}
              onChange={e => updateMaterialForm("name", e.target.value)}
            />
            <input
              type="text"
              placeholder="Характеристики"
              value={materialForm.characteristics}
              onChange={e => updateMaterialForm("characteristics", e.target.value)}
            />
            <input
              type="text"
              placeholder="Ед."
              value={materialForm.unit}
              onChange={e => updateMaterialForm("unit", e.target.value)}
            />
            <input
              type="number"
              min="0"
              step="0.01"
              placeholder="Цена"
              value={materialForm.price}
              onChange={e => updateMaterialForm("price", e.target.value)}
            />
            <input
              type="text"
              placeholder="Источник"
              value={materialForm.source}
              onChange={e => updateMaterialForm("source", e.target.value)}
            />
            <button onClick={handleCreateMaterial}>Сохранить</button>
          </div>

          <div className="materials-list">
            {materials.map(material => (
              <div key={material.id} className="material-row">
                <div>
                  <strong>{material.name}</strong>
                  <span>
                    {material.characteristics ? `${material.characteristics} · ` : ""}
                    {material.unit || "ед."} · {material.source || "без источника"}
                  </span>
                </div>
                <strong>{money(material.price)}</strong>
                <input
                  type="number"
                  min="1"
                  step="1"
                  inputMode="numeric"
                  pattern="[0-9]*"
                  value={quantityByMaterial[material.id] || 1}
                  onChange={e => setQuantityByMaterial(current => ({
                    ...current,
                    [material.id]: wholeQuantityInput(e.target.value),
                  }))}
                />
                <button className="ghost" onClick={() => handleAddMaterialToSmeta(material)}>
                  В смету
                </button>
              </div>
            ))}
          </div>
        </section>

        <section className="panel assistant">
          <h2>AI Ассистент</h2>
          <p className="muted">Настройки AI доступны только администратору.</p>
          <textarea
            placeholder="Например: создай смету 'СКУД офис', добавь монтажные работы 12 часов по 1800 или удали позицию #5"
            value={aiPrompt}
            onChange={e => setAiPrompt(e.target.value)}
          />
          <button onClick={handleAiRequest}>Выполнить</button>
          {aiResponse && <div className="assistant-answer">{aiResponse}</div>}
        </section>
      </section>

      {currentUser?.is_admin && (
        <section className="panel admin-settings-panel">
          <div className="section-title">
            <div>
              <h2>Админ-настройки</h2>
              <p>AI, доступы и управление пользователями</p>
            </div>
            <div className="title-actions">
              <button className="ghost" onClick={handleLoadModels}>
                Список моделей
              </button>
              <button onClick={handleSaveAiSettings}>Сохранить настройки</button>
            </div>
          </div>

          <div className="admin-settings-grid">
            <div className="admin-settings-column">
              <input
                type="text"
                placeholder="API URL"
                value={aiSettings.base_url}
                onChange={e => setAiSettings(current => ({ ...current, base_url: e.target.value }))}
              />
              <input
                type="password"
                placeholder={aiSettings.has_api_key ? `Ключ сохранён: ${aiSettings.masked_api_key}` : "API-ключ"}
                value={apiKeyInput}
                onChange={e => setApiKeyInput(e.target.value)}
              />
              <input
                type="text"
                placeholder="Модель"
                value={aiSettings.model}
                onChange={e => setAiSettings(current => ({ ...current, model: e.target.value }))}
              />
              {models.length > 0 && (
                <div className="models-compact">
                  <div className="model-price-table">
                    {models.slice(0, 80).map(model => (
                      <div
                        key={model.id}
                        className={model.id === aiSettings.model ? "price-row active" : "price-row"}
                        onClick={() => handleSelectModel(model.id)}
                      >
                        <span>{model.name || model.id}</span>
                        <small>{modelPrice(model)}</small>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>

            <div className="admin-settings-column">
              <textarea
                placeholder="Встроенный промпт ассистента"
                value={aiSettings.assistant_prompt || ""}
                onChange={e => setAiSettings(current => ({ ...current, assistant_prompt: e.target.value }))}
              />
            </div>
          </div>

          <div className="admin-two-col">
            <div className="admin-list">
              <div className="section-title compact">
                <div>
                  <h2>Пользователи</h2>
                  <p>{adminUsers.length} учетных записей</p>
                </div>
                {selectedAdminUser && <p className="muted">Выбран: {selectedAdminUser.email}</p>}
              </div>
              {adminUsers.map(user => (
                <div
                  key={user.id}
                  className={String(user.id) === String(adminSelectedUserId) ? "admin-user-row active" : "admin-user-row"}
                  role="button"
                  tabIndex={0}
                  onClick={() => handleSelectAdminUser(user.id)}
                  onKeyDown={e => {
                    if (e.key === "Enter" || e.key === " ") {
                      handleSelectAdminUser(user.id);
                    }
                  }}
                >
                  <div>
                    <strong>{user.email}</strong>
                    <span>
                      #{user.id}
                      {user.created_at ? ` · ${new Date(user.created_at).toLocaleDateString("ru-RU")}` : ""}
                    </span>
                  </div>
                  <div className="admin-user-actions">
                    <span className={user.is_admin ? "badge admin" : "badge"}>{user.is_admin ? "админ" : "пользователь"}</span>
                    {user.email !== "dboy@bk.ru" && (
                      <>
                        <button className="ghost" disabled={adminBusy} onClick={e => { e.stopPropagation(); handleToggleAdmin(user); }}>
                          {user.is_admin ? "Снять админа" : "Сделать админом"}
                        </button>
                        <button className="ghost danger" disabled={adminBusy} onClick={e => { e.stopPropagation(); handleDeleteUser(user); }}>
                          Удалить
                        </button>
                      </>
                    )}
                  </div>
                </div>
              ))}
            </div>

            <div className="admin-access">
              <div className="section-title compact">
                <div>
                  <h3>{selectedAdminUser ? "Доступы пользователя" : "Пользователи"}</h3>
                  <p className="muted">
                    {selectedAdminUser ? selectedAdminUser.email : "Выберите пользователя"}
                  </p>
                </div>
              </div>

              {selectedAdminUser ? (
                adminUserSmetas.length > 0 ? (
                  <>
                    <p className="muted">{selectedAdminUser.email} имеет доступ к {adminUserSmetas.length} сметам.</p>
                    {adminUserSmetas.map(smeta => (
                      <div key={smeta.id} className="admin-access-row">
                        <div>
                          <strong>{smeta.name}</strong>
                          <span>
                            {smeta.permission === "owner"
                              ? "владелец"
                              : smeta.permission === "edit"
                                ? "редактирование"
                                : smeta.permission === "view"
                                  ? "просмотр"
                                  : "админ-доступ"}
                          </span>
                        </div>
                        <strong>{money(smeta.total)}</strong>
                      </div>
                    ))}
                  </>
                ) : (
                  <p className="muted">У выбранного пользователя пока нет доступных смет.</p>
                )
              ) : (
                <p className="muted">Выберите пользователя, чтобы увидеть его сметы.</p>
              )}
            </div>
          </div>
        </section>
      )}
    </main>
  );
}

export default App;
