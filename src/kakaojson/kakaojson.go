package kakaojson

type Alimtalk struct {
	Message_type    string      `json:"message_type"`
	Serial_number   string      `json:"serial_number"`
	Sender_key      string      `json:"sender_key"`
	Phone_number    string      `json:"phone_number,omitempty"`
	App_user_id     string      `json:"app_user_id,omitempty"`
	Template_code   string      `json:"template_code"`
	Message         string      `json:"message"`
	Title           string      `json:"title,omitempty"`
	Response_method string      `json:"response_method"`
	Timeout         int         `json:"timeout,omitempty"`
	Attachment      AttachmentB `json:"attachment,omitempty"`
	Supplement      Supplement  `json:"supplement,omitempty"`
	Channel_key     string      `json:"channel_key,omitempty"`
	Price           int64       `json:"price,omitempty"`
	Currency_type   string      `json:"currency_type,omitempty"`
}

type Friendtalk struct {
	Message_type  string     `json:"message_type"`
	Serial_number string     `json:"serial_number"`
	Sender_key    string     `json:"sender_key"`
	Phone_number  string     `json:"phone_number,omitempty"`
	App_user_id   string     `json:"app_user_id,omitempty"`
	User_key      string     `json:"user_key,omitempty"`
	Message       string     `json:"message"`
	Ad_flag       string     `json:"ad_flag,omitempty"`
	Attachment    Attachment `json:"attachment,omitempty"`
	Header        string     `json:"header,omitempty"`
	Carousel      *FCarousel  `json:"carousel,omitempty"`
}

type Button struct {
	Name           string `json:"name"`
	Type           string `json:"type"`
	Scheme_android string `json:"scheme_android,omitempty"`
	Scheme_ios     string `json:"scheme_ios,omitempty"`
	Url_mobile     string `json:"url_mobile,omitempty"`
	Url_pc         string `json:"url_pc,omitempty"`
	Chat_extra     string `json:"chat_extra,omitempty"`
	Chat_event     string `json:"chat_event,omitempty"`
	Plugin_id      string `json:"plugin_id,omitempty"`
	Relay_id       string `json:"relay_id,omitempty"`
	Oneclick_id    string `json:"oneclick_id,omitempty"`
	Product_id     string `json:"product_id,omitempty"`
}

type CButton struct {
	Name         string `json:"name"`
	LinkType     string `json:"linktype"`
	LinkTypeName string `json:"linktypeName"`
	Ordering     string `json:"ordering"`
	LinkMo       string `json:"linkMo"`
	LinkPc       string `json:"linkPc"`
	LinkAnd      string `json:"linkAnd"`
	inkIos       string `json:"linkIos"`
	Pluginid     string `json:"pluginid"`
}

type Quickreply struct {
	Name           string `json:"name"`
	Type           string `json:"type"`
	Scheme_android string `json:"scheme_android,omitempty"`
	Scheme_ios     string `json:"scheme_ios,omitempty"`
	Url_mobile     string `json:"url_mobile,omitempty"`
	Url_pc         string `json:"url_pc,omitempty"`
	Chat_extra     string `json:"chat_extra,omitempty"`
	Chat_event     string `json:"chat_event,omitempty"`
}

type CQuickreply struct {
	Name     string `json:"name"`
	LinkType string `json:"linkType"`
	LinkMo   string `json:"linkMo"`
	LinkPc   string `json:"linkPc"`
	LinkAnd  string `json:"linkAnd"`
	inkIos   string `json:"linkIos"`
}

type Attachment struct {
	Buttons []Button     `json:"button,omitempty"`
	Ftimage *Image        `json:"image,omitempty"`
	Item    *AttItem      `json:"item,omitempty"`
	Coupon  *AttCoupon    `json:"coupon,omitempty"`
}

type AttachmentB struct {
	Buttons []Button `json:"button,omitempty"`
}

type Supplement struct {
	Quick_reply []Quickreply `json:"quick_reply,omitempty"`
}

type Image struct {
	Img_url  string `json:"img_url"`
	Img_link string `json:"img_link,omitempty"`
}

type AttCoupon struct {
	Title           string `json:"title"`
	Description     string `json:"description"`
	Url_pc          string `json:"url_pc,omitempty"`
	Url_mobile      string `json:"url_mobile,omitempty"`
	Scheme_android  string `json:"scheme_android,omitempty"`
	Scheme_ios      string `json:"scheme_ios,omitempty"`
}

type KakaoResponse struct {
	Code        string
	Received_at string
	Message     string
}

type PollingResponse struct {
	Code         string
	Response_id  int
	Response     PResponse
	Responsed_at string
	Message      string
}

type PResponse struct {
	Success []PResult
	Fail    []PResult
}

type PResult struct {
	Serial_number string
	Status        string
	Received_at   string
}

type FCarousel struct {
	List     []CarouselList `json:"list,omitempty"`
	Tail     CarouselTail `json:"tail,omitempty"`
}

type CarouselList struct {
	Header        string              `json:"header"`
	Message       string              `json:"message"`
	Attachment    CarouselAttachment  `json:"attachment"`
}

type TCarousel struct {
	List     []TCarouselList `json:"list"`
	Tail     CarouselTail `json:"tail,omitempty"`
}

type TCarouselList struct {
	Header        string              `json:"header"`
	Message       string              `json:"message"`
	Attachment    string              `json:"attachment,omitempty"`
}

type CarouselAttachment struct {
    Button  []CarouselButton `json:"button"` 
	Image     CarouselImage  `json:"image,omitempty"`
}

type CarouselTail struct {
	Url_pc          string              `json:"url_pc,omitempty"`
	Url_mobile      string              `json:"url_mobile,omitempty"`
	Scheme_ios      string              `json:"scheme_ios,omitempty"`
	Scheme_android  string              `json:"scheme_android,omitempty"`
}

type CarouselImage struct {
	Img_url  string `json:"img_url"`
	Img_link string `json:"img_link"`
}

type CarouselButton struct {
	Name           string `json:"name"`
	Type           string `json:"type"`
	Scheme_android string `json:"scheme_android,omitempty"`
	Scheme_ios     string `json:"scheme_ios,omitempty"`
	Url_mobile     string `json:"url_mobile,omitempty"`
	Url_pc         string `json:"url_pc,omitempty"`
}

type AttItem struct {
	AList   []ItemLists `json:"list,omitempty"`
}

type ItemLists struct {
	Title          string `json:"title"`
	Img_url        string `json:"img_url"`
	Scheme_android string `json:"scheme_android,omitempty"`
	Scheme_ios     string `json:"scheme_ios,omitempty"`
	Url_mobile     string `json:"url_mobile"`
	Url_pc         string `json:"url_pc,omitempty"`
}
