package kaocenter

type SenderCreate struct {
	YellowId     string `json:"yellowId" binding:"required"`
	CategoryCode string `json:"categoryCode" binding:"required"`
	ChannelKey   string `json:"channelKey,omitempty"`
}

type SenderDelete struct {
	SenderKey string `json:"senderKey" binding:"required"`
}

type TemplateCreate struct {
	SenderKey             string       				`json:"senderKey" binding:"required"`
	TemplateCode          string       				`json:"templateCode" binding:"required"`
	TemplateName          string       				`json:"templateName" binding:"required"`
	TemplateMessageType   string       				`json:"templateMessageType" binding:"required"`
	TemplateEmphasizeType string       				`json:"templateEmphasizeType" binding:"required"`
	TemplateContent       string       				`json:"templateContent" binding:"required"`
	TemplateExtra         string       				`json:"templateExtra,omitempty"`
	TemplateAd            string       				`json:"templateAd,omitempty"`
    TemplateImageName     string       				`json:"templateImageName,omitempty"`
    TemplateImageUrl      string      				`json:"templateImageUrl,omitempty"`
	TemplateTitle         string       				`json:"templateTitle,omitempty"`
	TemplateSubtitle      string       				`json:"templateSubtitle,omitempty"`
	TemplateHeader        string                  	`json:"templateHeader,omitempty"`
	TemplateItemHighlight TemplateItemHighlights  	`json:"templateItemHighlight,omitempty"`
	TemplateItem          TemplateItems           	`json:"templateItem,omitempty"`
	SenderKeyType         string       				`json:"senderKeyType,omitempty"`
	CategoryCode          string       				`json:"categoryCode,omitempty"`
	SecurityFlag          bool         				`json:"securityFlag,omitempty"`
	Buttons               []Button     				`json:"buttons,omitempty"`
	QuickReplies          []Quickreply 				`json:"quickReplies,omitempty"`
}

type Button struct {
	Name     string `json:"name"`
	LinkType string `json:"linkType"`
	Ordering int    `json:"ordering,omitempty"`
	LinkMo   string `json:"linkMo,omitempty"`
	LinkPc   string `json:"linkPc,omitempty"`
	LinkAnd  string `json:"linkAnd,omitempty"`
	LinkIos  string `json:"linkIos,omitempty"`
	PluginId string `json:"pluginId,omitempty"`
}

type Quickreply struct {
	Name     string `json:"name"`
	LinkType string `json:"linkType"`
	LinkMo   string `json:"linkMo,omitempty"`
	LinkPc   string `json:"linkPc,omitempty"`
	LinkAnd  string `json:"linkAnd,omitempty"`
	LinkIos  string `json:"linkIos,omitempty"`
}

type TemplateRequest struct {
	SenderKey     string `json:"senderKey" binding:"required"`
	TemplateCode  string `json:"templateCode" binding:"required"`
	SenderKeyType string `json:"senderKeyType,omitempty"`
}

type TemplateUpdate struct {
	SenderKey                string       `json:"senderKey" binding:"required"`
	TemplateCode             string       `json:"templateCode" binding:"required"`
	SenderKeyType            string       `json:"senderKeyType,omitempty"`
	NewSenderKey             string       `json:"newSenderKey" binding:"required"`
	NewTemplateCode          string       `json:"newTemplateCode" binding:"required"`
	NewTemplateName          string       `json:"newTemplateName" binding:"required"`
	NewTemplateMessageType   string       `json:"newTemplateMessageType" binding:"required"`
	NewTemplateEmphasizeType string       `json:"newTemplateEmphasizeType" binding:"required"`
	NewTemplateContent       string       `json:"newTemplateContent" binding:"required"`
	NewTemplateExtra         string       `json:"newTemplateExtra,omitempty"`
	NewTemplateAd            string       `json:"newTemplateAd,omitempty"`
	NewTemplateTitle         string       `json:"newTemplateTitle,omitempty"`
	NewTemplateSubtitle      string       `json:"newTemplateSubtitle,omitempty"`
	NewSenderKeyType         string       `json:"newSenderKeyType,omitempty"`
	NewCategoryCode          string       `json:"newCategoryCode,omitempty"`
	SecurityFlag             bool         `json:"securityFlag,omitempty"`
	Buttons                  []Button     `json:"buttons,omitempty"`
	QuickReplies             []Quickreply `json:"quickReplies,omitempty"`
}

type TemplateItemHighlights struct {
	title             string       `json:"title,omitempty"`
	description       string       `json:"description,omitempty"`
	imageUrl          string       `json:"imageUrl,omitempty"`
}

type TemplateItems struct {
	List          []TemplateItemList       `json:"list,omitempty"`
	Summary       []TemplateItemList       `json:"summary,omitempty"`
}

type TemplateItemList struct {
	Title             string       `json:"title,omitempty"`
	Description       string       `json:"description,omitempty"`
}


type TemplateComment struct {
	SenderKey     string `json:"senderKey" binding:"required"`
	TemplateCode  string `json:"templateCode" binding:"required"`
	SenderKeyType string `json:"senderKeyType,omitempty"`
	Comment       string `json:"comment" binding:"required"`
}

type TemplateCategoryUpdate struct {
	SenderKey     string `json:"senderKey" binding:"required"`
	TemplateCode  string `json:"templateCode" binding:"required"`
	SenderKeyType string `json:"senderKeyType,omitempty"`
	categoryCode  string `json:"comment" binding:"required"`
}

type GroupSenderAdd struct {
	GroupKey  string `json:"groupKey" binding:"required"`
	SenderKey string `json:"senderKey" binding:"required"`
}

type ChannelCreate struct {
	ChannelKey string `json:"channelKey" binding:"required"`
	Desc       string `json:"desc,omitempty"`
}

type ChannelSenders struct {
	ChannelKey string `json:"groupKey" binding:"required"`
	SenderKeys string `json:"senderKeys" binding:"required"`
}

type ChannelDelete struct {
	ChannelKey string `json:"groupKey" binding:"required"`
}

type PluginCallbackUrlCreate struct {
	SenderKey   string `json:"senderKey" binding:"required"`
	PluginType  string `json:"pluginType" binding:"required"`
	PluginId    string `json:"pluginId" binding:"required"`
	CallbackUrl string `json:"callbackUrl" binding:"required"`
}

type PluginCallbackUrlUpdate struct {
	SenderKey   string `json:"senderKey" binding:"required"`
	PluginId    string `json:"pluginId" binding:"required"`
	CallbackUrl string `json:"callbackUrl" binding:"required"`
}

type PluginCallbackUrlDelete struct {
	SenderKey string `json:"senderKey" binding:"required"`
	PluginId  string `json:"pluginId" binding:"required"`
}
